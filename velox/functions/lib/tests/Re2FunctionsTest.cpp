/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/functions/lib/Re2Functions.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <velox/type/Type.h>
#include <functional>
#include <optional>
#include <string>

#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/StringView.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions {

using namespace facebook::velox::test;

namespace {

std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return makeRe2Extract(name, inputArgs, config, /*emptyNoMatch=*/false);
}

class Re2FunctionsTest : public test::FunctionBaseTest {
 public:
  static void SetUpTestCase() {
    test::FunctionBaseTest::SetUpTestCase();
    exec::registerStatefulVectorFunction(
        "re2_match", re2MatchSignatures(), makeRe2Match);
    exec::registerStatefulVectorFunction(
        "re2_search", re2SearchSignatures(), makeRe2Search);
    exec::registerStatefulVectorFunction(
        "re2_extract", re2ExtractSignatures(), makeRegexExtract);
    exec::registerStatefulVectorFunction(
        "re2_extract_all", re2ExtractAllSignatures(), makeRe2ExtractAll);
    exec::registerStatefulVectorFunction("like", likeSignatures(), makeLike);
  }

 protected:
  const char* kLikePatternCharacterSet =
      "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ@#$^&*";
  const char* kWildcardCharacterSet = "_%";
  const char* kSingleWildcardCharacter = "_";
  const char* kAnyWildcardCharacter = "%";

  template <typename T>
  void testRe2ExtractAll(
      const std::vector<std::optional<std::string>>& inputs,
      const std::vector<std::optional<std::string>>& patterns,
      const std::vector<std::optional<T>>& groupIds,
      const std::vector<std::optional<std::vector<std::string>>>& output);

  std::string generateString(
      const char* characterSet,
      vector_size_t outputLength = 60) {
    vector_size_t arrLen = strlen(characterSet);
    std::string output;

    for (int i = 0; i < outputLength; i++) {
      output += characterSet[i % arrLen];
    }
    return output;
  }

  void testLike(
      const std::string& input,
      const std::string& pattern,
      std::optional<bool> expected) {
    testLike(input, pattern, std::nullopt, expected);
  }

  // If expected is std::nullopt, then we expect the last argument to be the
  // content of the expected error.
  void testLike(
      const std::string& input,
      const std::string& pattern,
      std::optional<char> escape,
      std::optional<bool> expected,
      const std::string& errorMessage = "") {
    {
      // Test literal path.
      auto eval = [&]() {
        return evaluateOnce<bool>(
            escape.has_value()
                ? fmt::format("like(c0, '{}', '{}')", pattern, escape.value())
                : fmt::format("like(c0, '{}')", pattern),
            std::optional<std::string>{input});
      };

      if (expected.has_value()) {
        EXPECT_EQ(eval().value(), *expected);
      } else {
        VELOX_ASSERT_THROW(eval(), errorMessage);
      }
    }

    // Test not literal path.
    auto inputView = StringView(input);
    auto patternView = StringView(pattern);
    auto escapeView = ""_sv;
    if (escape.has_value()) {
      escapeView = StringView(&escape.value(), 1);
    }

    auto flatInput = makeFlatVector<StringView>(
        {inputView, inputView, inputView, inputView});
    auto flatPattern = makeFlatVector<StringView>(
        {patternView, patternView, patternView, patternView});
    auto flatEscape = makeFlatVector<StringView>(
        {escapeView, escapeView, escapeView, escapeView});

    auto eval = [&]() {
      return evaluate(
          escape.has_value() ? "like(c0, c1, c2)" : "like(c0, c1)",
          makeRowVector({flatInput, flatPattern, flatEscape}));
    };

    if (expected.has_value()) {
      auto result = eval();
      assertEqualVectors(
          BaseVector::createConstant(BOOLEAN(), *expected, 4, pool()), result);
    } else {
      VELOX_ASSERT_THROW(eval(), errorMessage);
    }
  }
};

template <typename Table, typename Row, std::size_t... I>
void addRow(Table& dataset, const Row& row, std::index_sequence<I...>) {
  (std::get<I>(dataset).push_back(std::get<I>(row)), ...);
}

// Helper class to make it easier to write readable tests for vector
// functions.
//
// First, call this functor with test cases and assert the results are
// correct. The arguments provided will be captured and stored for use in
// testBatchAll(), which automatically constructs a batches from the test
// cases provided earlier.
//
// So for example:
//
// VectorFunctionTester<bool, double> isnan("isnan(c0)");
// EXPECT_EQ(isnan(1.0), false);
// EXPECT_EQ(isnan(kIsNan), true);
// EXPECT_EQ(std::nullopt, std::nullopt);
// isnan.testBatchAll();
//
// Currently, testBatchAll() generates three types of test cases:
//   1) A batch with all the provided test cases in the provided order.
//   2) One batch of 1024 identical rows per test case using FlatVectors.
//   3) One batch of 1024 identical rows per test case using ConstantVectors.
template <typename ReturnType, typename... T>
class VectorFunctionTester : public test::FunctionBaseTest {
 public:
  explicit VectorFunctionTester(std::string expr) : expr(expr) {}

  // Use to assert individual test cases.
  std::optional<ReturnType> operator()(const std::optional<T>&... args) {
    auto result = evaluateOnce<ReturnType>(expr, args...);
    addRow(this->args, std::tuple(args...), std::index_sequence_for<T...>());
    expected.push_back(result);
    return result;
  }

  // Generate, run, and verify function test cases tested earlier still pass
  // when in batches.
  void testBatchAll() {
    // Test a batch with all test cases in order.
    std::apply([&](auto... args) { testBatch(expected, args...); }, args);
    // Generate one batch per test case, with 1024 identical rows.
    for (int i = 0; i < expected.size(); ++i) {
      std::apply(
          [&](auto... args) {
            testBatch(
                std::vector(1024, expected[i]), std::vector(1024, args[i])...);
            testConstantBatch(expected[i], args[i]...);
          },
          args);
    }
  }

 private:
  using EvalReturnType = EvalType<ReturnType>;
  const std::string expr;
  std::tuple<std::vector<std::optional<T>>...> args;
  std::vector<std::optional<ReturnType>> expected;

  void testBatch(
      const std::vector<std::optional<ReturnType>>& expected,
      const std::vector<std::optional<T>>&... args) {
    auto actual = evaluate<SimpleVector<EvalReturnType>>(
        expr, makeRowVector({makeNullableFlatVector(args)...}));
    ASSERT_EQ(actual->size(), expected.size());
    for (int i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(
          actual->isNullAt(i) ? std::optional<ReturnType>{}
                              : ReturnType(actual->valueAt(i)),
          expected[i]);
    }
  }

  void testConstantBatch(
      const std::optional<ReturnType>& expected,
      std::optional<T>&... args) {
    constexpr std::size_t blocksize = 1024;
    auto actual = evaluate<SimpleVector<EvalReturnType>>(
        expr, makeRowVector({makeConstant(args, blocksize)...}));
    ASSERT_EQ(actual->size(), blocksize);
    for (int i = 0; i < blocksize; ++i) {
      EXPECT_EQ(
          actual->isNullAt(i) ? std::optional<ReturnType>{}
                              : ReturnType(actual->valueAt(i)),
          expected);
    }
  }

  // We inherit from FunctionBaseTest so that we can get access to the helpers
  // it defines, but since it is supposed to be a test fixture TestBody() is
  // declared pure virtual.  We must provide an implementation here.
  void TestBody() override {}
};

template <typename F>
void testRe2Match(F&& regexMatch) {
  // Empty string cases.
  EXPECT_EQ(true, regexMatch("", ""));
  EXPECT_EQ(false, regexMatch("abc", ""));
  EXPECT_EQ(false, regexMatch("", "abc"));
  // matching case.
  EXPECT_EQ(true, regexMatch("abc", "abcd*"));
  EXPECT_EQ(false, regexMatch("abd", "abcd*"));
  // Prefix match.
  EXPECT_EQ(false, regexMatch("abc ", "abcd*"));
  // Suffix match.
  EXPECT_EQ(false, regexMatch(" abc", "abcd*"));
  // Invalid regex.
  EXPECT_THROW(regexMatch("abc", "*"), VeloxException);
  // Null cases.
  EXPECT_EQ(std::nullopt, regexMatch(std::nullopt, ".*"));
  EXPECT_EQ(std::nullopt, regexMatch("", std::nullopt));
}

TEST_F(Re2FunctionsTest, regexMatchConstantPattern) {
  // If the pattern is a null literal, the resulting analyzer type will be
  // unknown. This is not a useful test, since the respective databases's
  // mapping into velox is expected to provide types explicitly. Therefore we
  // just return null directly in this case.
  testRe2Match([&](std::optional<std::string> str,
                   std::optional<std::string> pattern) {
    return pattern
        ? evaluateOnce<bool>("re2_match(c0, '" + *pattern + "')", str, pattern)
        : std::nullopt;
  });
}

TEST_F(Re2FunctionsTest, regexMatch) {
  testRe2Match(
      [&](std::optional<std::string> str, std::optional<std::string> pattern) {
        return evaluateOnce<bool>("re2_match(c0, c1)", str, pattern);
      });
}

TEST_F(Re2FunctionsTest, regexMatchBatch) {
  VectorFunctionTester<bool, std::string, std::string> re2Match(
      "re2_match(c0, c1)");
  testRe2Match(re2Match);
  re2Match.testBatchAll();
}

template <typename F>
void testRe2Search(F&& regexSearch) {
  // Empty string cases.
  EXPECT_EQ(true, regexSearch("", ""));
  EXPECT_EQ(true, regexSearch("abc", ""));
  EXPECT_EQ(false, regexSearch("", "abc"));
  // searching case.
  EXPECT_EQ(true, regexSearch("abc", "abcd*"));
  EXPECT_EQ(false, regexSearch("abd", "abcd*"));
  // Prefix search.
  EXPECT_EQ(true, regexSearch("abc ", "abcd*"));
  // Suffix search.
  EXPECT_EQ(true, regexSearch(" abc", "abcd*"));
  // Invalid regex.
  EXPECT_THROW(regexSearch("abc", "*"), VeloxException);
  // Null cases.
  EXPECT_EQ(std::nullopt, regexSearch(std::nullopt, ".*"));
  EXPECT_EQ(std::nullopt, regexSearch("", std::nullopt));
}

TEST_F(Re2FunctionsTest, regexSearchConstantPattern) {
  testRe2Search([&](std::optional<std::string> str,
                    std::optional<std::string> pattern) {
    return pattern
        ? evaluateOnce<bool>("re2_search(c0, '" + *pattern + "')", str, pattern)
        : std::nullopt;
    ;
  });
}

TEST_F(Re2FunctionsTest, regexSearch) {
  testRe2Search(
      [&](std::optional<std::string> str, std::optional<std::string> pattern) {
        return evaluateOnce<bool>("re2_search(c0, c1)", str, pattern);
      });
}

TEST_F(Re2FunctionsTest, regexSearchBatch) {
  VectorFunctionTester<bool, std::string, std::string> re2Search(
      "re2_search(c0, c1)");
  testRe2Search(re2Search);
  re2Search.testBatchAll();
}

template <typename F>
void testRe2Extract(F&& regexExtract) {
  // Regex with no subgroup matches.
  EXPECT_THROW(regexExtract(" 123 ", "\\d+", -1), VeloxException);
  EXPECT_EQ(regexExtract("123", "\\d+", 0), "123");
  EXPECT_EQ(regexExtract(" 123 ", "\\d+", 0), "123");
  EXPECT_EQ(
      regexExtract("stringwithmorethan16chars", "^[^\\d]+", 0),
      "stringwithmorethan");
  EXPECT_THROW(regexExtract(" 123 ", "\\d+", 1), VeloxException);
  // Regex with subgroup matches.
  EXPECT_THROW(regexExtract(" 123 ", "(\\d+) ", -1), VeloxException);
  EXPECT_EQ(regexExtract(" 123 ", "(\\d+)", 0), "123");
  EXPECT_EQ(regexExtract(" 123 ", "(\\d+)", 1), "123"); // Partial match.
  EXPECT_EQ(regexExtract(" 123 ", "(\\d+) ", 1), "123"); // Suffix match.
  EXPECT_EQ(regexExtract(" 123 ", " (\\d+)", 1), "123"); // Prefix match
  EXPECT_EQ(regexExtract(" 123 ", " (\\d+) ", 1), "123"); // Full match.
  // Test with string and match that will not be inlined.
  EXPECT_EQ(
      regexExtract("stringwithmorethan16chars", "^([^\\d]+)\\d", 1),
      "stringwithmorethan");
  EXPECT_THROW(regexExtract(" 123 ", " (\\d+)", 2), VeloxException);
  // Regex doesn't match.
  EXPECT_THROW(regexExtract("abc", "\\d+", -1), VeloxException);
  EXPECT_EQ(regexExtract("abc", "\\d+", 0), std::nullopt);
  EXPECT_THROW(regexExtract("abc", "\\d+", 1), VeloxException);
  // Invalid regex.
  EXPECT_THROW(regexExtract("abc", "*", 0), VeloxException);
  // NULL cases.
  EXPECT_EQ(regexExtract(std::nullopt, "\\d+", 0), std::nullopt);
  EXPECT_EQ(regexExtract(" 123 ", std::nullopt, 0), std::nullopt);
  EXPECT_EQ(regexExtract(" 123 ", "\\d+", std::nullopt), std::nullopt);
}

TEST_F(Re2FunctionsTest, regexExtract) {
  testRe2Extract([&](std::optional<std::string> str,
                     std::optional<std::string> pattern,
                     std::optional<int> group) {
    return evaluateOnce<std::string>(
        "re2_extract(c0, c1, c2)", str, pattern, group);
  });
}

TEST_F(Re2FunctionsTest, regexExtractBigintGroupId) {
  testRe2Extract([&](std::optional<std::string> str,
                     std::optional<std::string> pattern,
                     std::optional<int64_t> group) {
    return evaluateOnce<std::string>(
        "re2_extract(c0, c1, c2)", str, pattern, group);
  });
}

TEST_F(Re2FunctionsTest, regexExtractBatch) {
  VectorFunctionTester<std::string, std::string, std::string, int> re2Extract{
      "re2_extract(c0, c1, c2)"};
  testRe2Extract(re2Extract);
  re2Extract.testBatchAll();
}

TEST_F(Re2FunctionsTest, regexExtractConstantPattern) {
  testRe2Extract([&](std::optional<std::string> str,
                     std::optional<std::string> pattern,
                     std::optional<int> group) {
    return pattern ? evaluateOnce<std::string>(
                         "re2_extract(c0, '" + *pattern + "', c1)", str, group)
                   : std::nullopt;
  });
}

TEST_F(Re2FunctionsTest, regexExtractNoGroupId) {
  auto extract =
      ([&](std::optional<std::string> str, std::optional<std::string> pattern) {
        return evaluateOnce<std::string>("re2_extract(c0, c1)", str, pattern);
      });

  EXPECT_EQ(extract("a1 b2 c3", "\\d+"), "1");
  EXPECT_EQ(extract("a b245 c3", "\\d+"), "245");
}

TEST_F(Re2FunctionsTest, regexExtractConstantPatternNoGroupId) {
  auto extract =
      ([&](std::optional<std::string> str, std::optional<std::string> pattern) {
        return evaluateOnce<std::string>(
            "re2_extract(c0, '" + *pattern + "')", str);
      });

  EXPECT_EQ(extract("a1 b2 c3", "\\d+"), "1");
  EXPECT_EQ(extract("a b245 c3", "\\d+"), "245");
}

TEST_F(Re2FunctionsTest, likePattern) {
  testLike("abc", "%b%", true);
  testLike("bcd", "%b%", true);
  testLike("cde", "%b%", false);
  testLike("cde", "def", false);

  testLike("abc", "_b%", true);
  testLike("bcd", "_b%", false);
  testLike("cde", "_b%", false);

  testLike("abc", "b%", false);
  testLike("bcd", "b%", true);
  testLike("cde", "b%", false);

  testLike("abc", "B%", false);
  testLike("bcd", "B%", false);
  testLike("cde", "B%", false);

  testLike("stringwithmorethan16chars", "string%", true);
  testLike("stringwithmorethan16chars", "stringwithmorethan16chars", true);
  testLike("stringwithmorethan16chars", "stringwithlessthan16chars", false);

  testLike(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 abc",
      "\u4FE1\u5FF5 \u7231%",
      true);
  testLike("\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ", "\u4FE1%\u7231%", true);
  testLike(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ",
      "\u7231\u4FE1%\u7231%",
      false);

  testLike("abc", "MEDIUM POLISHED%", false);
}

TEST_F(Re2FunctionsTest, likeDeterminePatternKind) {
  auto testPattern = [&](StringView pattern,
                         PatternKind patternKind,
                         vector_size_t length,
                         StringView fixedPattern = "") {
    PatternMetadata patternMetadata = determinePatternKind(pattern);
    EXPECT_EQ(patternMetadata.patternKind, patternKind);
    EXPECT_EQ(patternMetadata.length, length);
    EXPECT_EQ(patternMetadata.fixedPattern, fixedPattern);
  };

  testPattern("_", PatternKind::kExactlyN, 1);
  testPattern("____", PatternKind::kExactlyN, 4);
  testPattern("%", PatternKind::kAtLeastN, 0);
  testPattern("%%%", PatternKind::kAtLeastN, 0);
  testPattern("__%%__", PatternKind::kAtLeastN, 4);
  testPattern("%_%%", PatternKind::kAtLeastN, 1);

  testPattern("presto", PatternKind::kFixed, 6);
  testPattern("hello", PatternKind::kFixed, 5);
  testPattern("a", PatternKind::kFixed, 1);
  testPattern("helloPrestoWorld", PatternKind::kFixed, 16);
  testPattern("aBcD", PatternKind::kFixed, 4);

  testPattern("presto%", PatternKind::kPrefix, 6);
  testPattern("hello%%", PatternKind::kPrefix, 5);
  testPattern("a%", PatternKind::kPrefix, 1);
  testPattern("helloPrestoWorld%%%", PatternKind::kPrefix, 16);
  testPattern("aBcD%", PatternKind::kPrefix, 4);

  testPattern("%presto", PatternKind::kSuffix, 6);
  testPattern("%%hello", PatternKind::kSuffix, 5);
  testPattern("%a", PatternKind::kSuffix, 1);
  testPattern("%%%helloPrestoWorld", PatternKind::kSuffix, 16);
  testPattern("%aBcD", PatternKind::kSuffix, 4);

  testPattern("%presto%%", PatternKind::kSubstring, 0, "presto");
  testPattern("%%hello%", PatternKind::kSubstring, 0, "hello");
  testPattern("%%%aAb\n%", PatternKind::kSubstring, 0, "aAb\n");
  testPattern(
      "%helloPrestoWorld%%%", PatternKind::kSubstring, 0, "helloPrestoWorld");

  testPattern("_b%%__", PatternKind::kGeneric, 0);
  testPattern("%_%p", PatternKind::kGeneric, 0);
  testPattern("aBcD_", PatternKind::kGeneric, 0);
  testPattern("%aBc_D%", PatternKind::kGeneric, 0);
  testPattern("aBcD%%e%", PatternKind::kGeneric, 0);
  testPattern("aBc_D%%", PatternKind::kGeneric, 0);
  testPattern("%%_%aBcD", PatternKind::kGeneric, 0);
  testPattern("%%a%%BcD", PatternKind::kGeneric, 0);
  testPattern("%%aBcD%_%", PatternKind::kGeneric, 0);
  testPattern("foo%bar", PatternKind::kGeneric, 0);
  testPattern("_aBcD", PatternKind::kGeneric, 0);
}

TEST_F(Re2FunctionsTest, likePatternWildcard) {
  testLike("", "", true);
  testLike("", "%", true);
  testLike("abc", "%%%%", true);
  testLike("abc", "%%", true);
  testLike("abc", "%_%_%", true);
  testLike("abc", "_%_%_%", true);
  testLike("a", "_", true);
  testLike("ab", "__", true);
  testLike("abc", "___", true);

  testLike("", "_", false);
  testLike("ab", "_", false);
  testLike("abcd", "___", false);
  testLike("acb", "%ab_%", false);
  testLike("abcd", "_abc%d", false);
  testLike("abcd", "%ab_c%", false);

  testLike(
      generateString(kLikePatternCharacterSet),
      generateString(kWildcardCharacterSet),
      true);
  testLike(
      generateString(kLikePatternCharacterSet),
      generateString(kSingleWildcardCharacter, 65),
      false);

  // Test that % matches newline.
  testLike("\nabcde\n", "%bcd%", true);
  testLike("\nabcd", "%bcd", true);
  testLike("bcde\n", "bcd%", true);
  testLike("\nabcde\n", "bcd%", false);
  testLike("\nabcde\n", "%bcd", false);
  testLike("\nabcde\n", "%bcf%", false);
}

TEST_F(Re2FunctionsTest, likePatternFixed) {
  testLike("", "", true);
  testLike("abcde", "abcde", true);
  testLike("ABCDE", "ABCDE", true);
  testLike("abcde", "uvwxy", false);
  testLike("ABCDE", "abcde", false);
  testLike("abc", "%%%%", true);
  testLike("abc", "%%", true);
  testLike("abc", "%_%_%", true);
  testLike("abc", "_%_%_%", true);
  testLike("a", "_", true);
  testLike("", "_", false);
  testLike("ab", "_", false);
  testLike("abc", "__%_%_", false);
  testLike("abcd", "_c%_%_", false);
  testLike("\nab\ncd\n", "\nab\ncd\n", true);
  testLike("abcd\n", "abcd\n", true);
  testLike("\nabcd", "\nabcd", true);
  testLike("\tab\ncd\b", "\tab\ncd\b", true);
  testLike("\nabcd\n", "\nabc\nd\n", false);
  testLike("\nab\tcd\b", "\nabcd\b", false);

  std::string input = generateString(kLikePatternCharacterSet, 66);
  testLike(input, input, true);
}

TEST_F(Re2FunctionsTest, likePatternPrefix) {
  testLike("", "%", true);
  testLike("", "%%", true);
  testLike("abcde", "abcd%", true);
  testLike("ABCDE", "ABC%", true);
  testLike("abcde", "abcd%%", true);
  testLike("ABCDE", "ABC_%", true);
  testLike("abcde", "ab%", true);
  testLike("ABCDE", "AB%", true);
  testLike("abcde", "ab_%", true);
  testLike("ABCDE", "AB%%", true);
  testLike("", "_%", false);
  testLike("abcde", "abce%", false);
  testLike("ABCDE", "ABD%", false);
  testLike("abcde", "abce%_", false);
  testLike("ABCDE", "ABD%%", false);
  testLike("abcde", "ad%", false);
  testLike("ABCDE", "abc%", false);
  testLike("abcde", "ad%%", false);
  testLike("ABCDE", "abc_%", false);
  testLike("ABC\n", "ABC_", true);
  testLike("ABC\n", "ABC_%", true);
  testLike("\nABC\n", "_ABC%", true);
  testLike("\nabcde\n", "\nab%", true);
  testLike("\nab\ncde\n", "\nab\n%", true);
  testLike("\nabc\nde\n", "ab\nc%", false);
  testLike("\nabc\nde\n", "abc%", false);

  std::string input = generateString(kLikePatternCharacterSet, 66);
  testLike(input, input + generateString(kAnyWildcardCharacter), true);
}

TEST_F(Re2FunctionsTest, likePatternSuffix) {
  testLike("", "%", true);
  testLike("abcde", "%bcde", true);
  testLike("ABCDE", "%CDE", true);
  testLike("abcde", "%%cde", true);
  testLike("ABCDE", "%%DE", true);
  testLike("abcde", "%de", true);
  testLike("ABCDE", "%DE", true);
  testLike("abcde", "%%e", true);
  testLike("ABCDE", "%%E", true);
  testLike("", "%_", false);
  testLike("abcde", "%ccde", false);
  testLike("ABCDE", "%BDE", false);
  testLike("abcde", "%%ccde", false);
  testLike("ABCDE", "%%BDE", false);
  testLike("abcde", "%be", false);
  testLike("ABCDE", "%de", false);
  testLike("abcde", "%%ce", false);
  testLike("ABCDE", "%%e", false);
  testLike("\nabc\nde\n", "%\nde\n", true);
  testLike("\nabcde\n", "%%de\n", true);
  testLike("\nabc\tde\b", "%\tde\b", true);
  testLike("\nabcde\t", "%%de\t", true);
  testLike("\nabc\nde\t", "%bc_de_", true);
  testLike("\nabcde\n", "%de\b", false);
  testLike("\nabcde\n", "%d\n", false);
  testLike("\nabcde\n", "%e_\n", false);

  std::string input = generateString(kLikePatternCharacterSet, 65);
  testLike(input, generateString(kAnyWildcardCharacter) + input, true);
}

TEST_F(Re2FunctionsTest, likeSubstringPattern) {
  testLike("abcde", "%abcde%%", true);
  testLike("abcde", "%%bcde%", true);
  testLike("ABCDE", "%%CD%%%", true);
  testLike("abcde", "%%%c%", true);
  testLike("ABCDE", "%%E%%", true);
  testLike("abcde", "%a%", true);
  testLike("ABCDE", "%DE%%", true);
  testLike("abcde", "%%%%c%%%%", true);
  testLike("ABCDE", "%%A%%%", true);
  testLike("", "%_%", false);
  testLike("abcde", "%cc%", false);
  testLike("ABCDE", "%BD%%", false);
  testLike("abcde", "%%ccd%%%", false);
  testLike("ABCDE", "%%BDE%", false);
  testLike("abcde", "%%be%", false);
  testLike("ABCDE", "%de%%", false);
  testLike("abcde", "%cb%%", false);
  testLike("ABCDE", "%%Ab%", false);
  testLike("\nabc\nde\n", "%\nde%%", true);
  testLike("\nabcde\n", "%%de%", true);
  testLike("\nabc\tde\b", "%%%d%%", true);
  testLike("\nabcde\t", "%%e\t%%", true);
  testLike("\nabcde\n", "%d_e\b%%", false);
  testLike("\nabcde\n", "%%d\n%", false);
  testLike("\nabcde\n", "%%e_\n%%", false);

  std::string input = generateString(kLikePatternCharacterSet, 65);
  testLike(
      input,
      generateString(kAnyWildcardCharacter) + input +
          generateString(kAnyWildcardCharacter),
      true);
}

TEST_F(Re2FunctionsTest, nullConstantPatternOrEscape) {
  // Test null pattern.
  ASSERT_TRUE(
      !evaluateOnce<bool>("like('a', cast (null as varchar))").has_value());

  // Test null escape.
  ASSERT_TRUE(
      !evaluateOnce<bool>("like('a', 'a', cast(null as varchar))").has_value());
}

TEST_F(Re2FunctionsTest, likePatternAndEscape) {
  testLike("a_c", "%#_%", '#', true);
  testLike("_cd", "%#_%", '#', true);
  testLike("cde", "%#_%", '#', false);

  testLike("a%c", "%#%%", '#', true);
  testLike("%cd", "%#%%", '#', true);
  testLike("cde", "%#%%", '#', false);

  testLike(
      "abcd",
      "a#}#+",
      '#',
      std::nullopt,
      "Escape character must be followed by '%', '_' or the escape character itself");
}

template <typename T>
void Re2FunctionsTest::testRe2ExtractAll(
    const std::vector<std::optional<std::string>>& inputs,
    const std::vector<std::optional<std::string>>& patterns,
    const std::vector<std::optional<T>>& groupIds,
    const std::vector<std::optional<std::vector<std::string>>>& output) {
  std::string constantPattern = "";
  std::string constantGroupId = "";
  std::string expression = "";

  auto result = [&] {
    auto input = makeFlatVector<StringView>(
        inputs.size(),
        [&inputs](vector_size_t row) {
          return inputs[row] ? StringView(*inputs[row]) : StringView();
        },
        [&inputs](vector_size_t row) { return !inputs[row].has_value(); });

    auto pattern = makeFlatVector<StringView>(
        patterns.size(),
        [&patterns](vector_size_t row) {
          return patterns[row] ? StringView(*patterns[row]) : StringView();
        },
        [&patterns](vector_size_t row) { return !patterns[row].has_value(); });
    if (patterns.size() == 1) {
      // Constant pattern
      constantPattern = std::string(", '") + patterns[0].value() + "'";
    }

    auto groupId = makeFlatVector<T>(
        groupIds.size(),
        [&groupIds](vector_size_t row) {
          return groupIds[row] ? *groupIds[row] : 0;
        },
        [&groupIds](vector_size_t row) { return !groupIds[row].has_value(); });
    if (groupIds.size() == 1) {
      // constant groupId
      constantGroupId = std::string(", ") + std::to_string(groupIds[0].value());
    }

    if (!constantPattern.empty()) {
      if (groupIds.empty()) {
        // Case 1: constant pattern, no groupId
        // for example: expression = re2_extract_all(c0, '(\\d+)([a-z]+)')
        expression = std::string("re2_extract_all(c0") + constantPattern + ")";
        return evaluate(expression, makeRowVector({input}));
      } else if (!constantGroupId.empty()) {
        // Case 2: constant pattern, constant groupId
        // for example: expression = re2_extract_all(c0, '(\\d+)([a-z]+)', 1)
        expression = std::string("re2_extract_all(c0") + constantPattern +
            constantGroupId + ")";
        return evaluate(expression, makeRowVector({input}));
      } else {
        // Case 3: constant pattern, variable groupId
        // for example: expression = re2_extract_all(c0, '(\\d+)([a-z]+)', c1)
        expression =
            std::string("re2_extract_all(c0") + constantPattern + ", c1)";
        return evaluate(expression, makeRowVector({input, groupId}));
      }
    }

    // Case 4: variable pattern, no groupId
    if (groupIds.empty()) {
      // for example: expression = re2_extract_all(c0, c1)
      expression = std::string("re2_extract_all(c0, c1)");
      return evaluate(expression, makeRowVector({input, pattern}));
    }

    // Case 5: variable pattern, constant groupId
    if (!constantGroupId.empty()) {
      // for example: expression = re2_extract_all(c0, c1, 0)
      expression =
          std::string("re2_extract_all(c0, c1") + constantGroupId + ")";
      return evaluate(expression, makeRowVector({input, pattern}));
    }

    // Case 6: variable pattern, variable groupId
    expression = std::string("re2_extract_all(c0, c1, c2)");
    return evaluate(expression, makeRowVector({input, pattern, groupId}));
  }();

  // Creating vectors for output string vectors
  auto sizeAtOutput = [&output](vector_size_t row) {
    return output[row] ? output[row]->size() : 0;
  };
  auto valueAtOutput = [&output](vector_size_t row, vector_size_t idx) {
    return output[row] ? StringView(output[row]->at(idx)) : StringView("");
  };
  auto nullAtOutput = [&output](vector_size_t row) {
    return !output[row].has_value();
  };
  auto expectedResult = makeArrayVector<StringView>(
      output.size(), sizeAtOutput, valueAtOutput, nullAtOutput);

  // Checking the results
  assertEqualVectors(expectedResult, result);
}

TEST_F(Re2FunctionsTest, regexExtractAllConstantPatternNoGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ", "123a 2b     14m", "123a2b14m"};
  const std::vector<std::optional<std::string>> constantPattern = {
      "(\\d+)([a-z]+)"};
  const std::vector<std::optional<int32_t>> intNoGroupId = {};
  const std::vector<std::optional<int64_t>> bigNoGroupId = {};
  const std::vector<std::optional<std::vector<std::string>>> expectedOutputs = {
      {{"123a", "2b", "14m"}},
      {{"123a", "2b", "14m"}},
      {{"123a", "2b", "14m"}}};

  testRe2ExtractAll(inputs, constantPattern, intNoGroupId, expectedOutputs);
  testRe2ExtractAll(inputs, constantPattern, bigNoGroupId, expectedOutputs);
}

TEST_F(Re2FunctionsTest, regexExtractAllConstantPatternConstantGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ", "123a 2b     14m", "123a2b14m"};
  const std::vector<std::optional<std::string>> constantPattern = {
      "(\\d+)([a-z]+)"};
  const std::vector<std::optional<int32_t>> intGroupIds = {1};
  const std::vector<std::optional<int64_t>> bigGroupIds = {1};
  const std::vector<std::optional<std::vector<std::string>>> expectedOutputs = {
      {{"123", "2", "14"}}, {{"123", "2", "14"}}, {{"123", "2", "14"}}};

  testRe2ExtractAll(inputs, constantPattern, intGroupIds, expectedOutputs);
  testRe2ExtractAll(inputs, constantPattern, bigGroupIds, expectedOutputs);
}

TEST_F(Re2FunctionsTest, regexExtractAllConstantPatternVariableGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ",
      "123a 2b     14m",
      "123a2b14m",
      "123a2b14m",
      "123a2b14m",
      "123a2b14m"};
  const std::vector<std::optional<std::string>> constantPattern = {
      "(\\d+)([a-z]+)"};
  const std::vector<std::optional<int32_t>> intGroupIds = {0, 1, 2, 0, 1, 2};
  const std::vector<std::optional<int64_t>> bigGroupIds = {0, 1, 2, 0, 1, 2};
  const std::vector<std::optional<std::vector<std::string>>> expectedOutputs = {
      {{"123a", "2b", "14m"}},
      {{"123", "2", "14"}},
      {{"a", "b", "m"}},
      {{"123a", "2b", "14m"}},
      {{"123", "2", "14"}},
      {{"a", "b", "m"}}};

  testRe2ExtractAll(inputs, constantPattern, intGroupIds, expectedOutputs);
  testRe2ExtractAll(inputs, constantPattern, bigGroupIds, expectedOutputs);
}

TEST_F(Re2FunctionsTest, regexExtractAllVariablePatternNoGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ", "123a 2b     14m", "123a2b14m"};
  const std::vector<std::optional<std::string>> patterns = {
      "(\\d+)([a-z]+)", "([a-z]+)", "(\\d+)"};
  const std::vector<std::optional<int32_t>> intGroupIds = {};
  const std::vector<std::optional<int64_t>> bigGroupIds = {};
  const std::vector<std::optional<std::vector<std::string>>> expectedOutputs = {
      {{"123a", "2b", "14m"}}, {{"a", "b", "m"}}, {{"123", "2", "14"}}};

  testRe2ExtractAll(inputs, patterns, intGroupIds, expectedOutputs);
  testRe2ExtractAll(inputs, patterns, bigGroupIds, expectedOutputs);
}

TEST_F(Re2FunctionsTest, regexExtractAllVariablePatternConstantGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ", "a123 b2     m14", "123a2b14m"};
  const std::vector<std::optional<std::string>> patterns = {
      "(\\d+)([a-z]+)", "([a-z]+)(\\d+)", "(\\d+)([a-b]+)"};
  const std::vector<std::optional<int32_t>> intGroupIds = {0};
  const std::vector<std::optional<int64_t>> bigGroupIds = {1};

  const std::vector<std::optional<std::vector<std::string>>>
      expectedOutputsInt = {
          {{"123a", "2b", "14m"}}, {{"a123", "b2", "m14"}}, {{"123a", "2b"}}};
  const std::vector<std::optional<std::vector<std::string>>>
      expectedOutputsBigInt = {
          {{"123", "2", "14"}}, {{"a", "b", "m"}}, {{"123", "2"}}};

  testRe2ExtractAll(inputs, patterns, intGroupIds, expectedOutputsInt);
  testRe2ExtractAll(inputs, patterns, bigGroupIds, expectedOutputsBigInt);
}

TEST_F(Re2FunctionsTest, regexExtractAllVariablePatternVariableGroupId) {
  const std::vector<std::optional<std::string>> inputs = {
      "  123a   2b   14m  ", "a123 b2     m14", "123a2b14m"};
  const std::vector<std::optional<std::string>> patterns = {
      "(\\d+)([a-z]+)", "([a-z]+)(\\d+)", "(\\d+)([a-b]+)"};
  const std::vector<std::optional<int32_t>> intGroupIds = {0, 1, 2};
  const std::vector<std::optional<int64_t>> bigGroupIds = {1, 2, 0};

  const std::vector<std::optional<std::vector<std::string>>>
      expectedOutputsInt = {
          {{"123a", "2b", "14m"}}, {{"a", "b", "m"}}, {{"a", "b"}}};
  const std::vector<std::optional<std::vector<std::string>>>
      expectedOutputsBigInt = {
          {{"123", "2", "14"}}, {{"123", "2", "14"}}, {{"123a", "2b"}}};

  testRe2ExtractAll(inputs, patterns, intGroupIds, expectedOutputsInt);
  testRe2ExtractAll(inputs, patterns, bigGroupIds, expectedOutputsBigInt);
}

TEST_F(Re2FunctionsTest, regexExtractAllNoMatch) {
  const std::vector<std::optional<int32_t>> noGroupId = {};
  const std::vector<std::optional<int32_t>> groupIds0 = {0};

  testRe2ExtractAll({""}, {"[0-9]+"}, noGroupId, {{{}}});
  testRe2ExtractAll({"(╯°□°)╯︵ ┻━┻"}, {"[0-9]+"}, noGroupId, {{{}}});
  testRe2ExtractAll({"abcde"}, {"[0-9]+"}, groupIds0, {{{}}});
  testRe2ExtractAll(
      {"rYBKVn6DnfSI2an4is4jbvf4btGpV"},
      {"81jnp58n31BtMdlUsP1hiF4QWSYv411"},
      noGroupId,
      {{{}}});
  testRe2ExtractAll(
      {"rYBKVn6DnfSI2an4is4jbvf4btGpV"},
      {"81jnp58n31BtMdlUsP1hiF4QWSYv411"},
      groupIds0,
      {{{}}});

  // Test empty pattern.
  testRe2ExtractAll<int32_t>(
      {"abcdef"}, {""}, {}, {{{"", "", "", "", "", "", ""}}});
}

TEST_F(Re2FunctionsTest, regexExtractAllBadArgs) {
  const auto eval = [&](std::optional<std::string> str,
                        std::optional<std::string> pattern,
                        std::optional<int32_t> groupId) {
    const std::string expression = "re2_extract_all(c0, c1, c2)";
    return evaluateOnce<std::string>(expression, str, pattern, groupId);
  };
  EXPECT_THROW(eval("123", std::nullopt, 0), VeloxException);
  EXPECT_THROW(eval(std::nullopt, "(\\d+)", 0), VeloxException);
  EXPECT_THROW(eval("", "", 123), VeloxException);
  EXPECT_THROW(eval("123", "", 99), VeloxException);
  EXPECT_THROW(eval("123", "(\\d+)", 1), VeloxException);
  EXPECT_THROW(eval("123", "[a-z]+", 1), VeloxException);
}

TEST_F(Re2FunctionsTest, tryException) {
  // Assert we throw without try.
  VELOX_ASSERT_THROW(
      evaluateOnce<std::string>(
          "re2_extract(c0, c1)",
          std::optional("123"),
          std::optional("V)%(&r_b2o&Xw")),
      "invalid regular expression");

  auto patternVector = makeConstant("V)%(&r_b2o&Xw", 3);
  auto oneGoodPatternVector =
      makeNullableFlatVector<StringView>({"V)%(&r_b2o&Xw", ".*", std::nullopt});
  auto stringVector = makeFlatVector<std::string>({"abc", "mno", "pqr"});
  auto input = makeRowVector({stringVector, patternVector});
  auto oneGoodInput = makeRowVector({stringVector, oneGoodPatternVector});

  // Assert we can handle trys safely for re2_extract.
  {
    auto result = evaluate("try(re2_extract(c0, c1))", input);
    assertEqualVectors(makeNullConstant(TypeKind::VARCHAR, 3), result);

    // Atleast one non null result.
    result = evaluate("try(re2_extract(c0, c1))", oneGoodInput);
    assertEqualVectors(
        makeNullableFlatVector<StringView>({std::nullopt, "mno", std::nullopt}),
        result);
  }

  // Try the same with re2_match.
  {
    auto result = evaluate("try(re2_match(c0, c1))", input);
    assertEqualVectors(makeNullConstant(TypeKind::BOOLEAN, 3), result);

    // At least one non null result.
    result = evaluate("try(re2_match(c0, c1))", oneGoodInput);
    assertEqualVectors(
        makeNullableFlatVector<bool>({std::nullopt, true, std::nullopt}),
        result);
  }

  // Try the same with re2_search.
  {
    auto result = evaluate("try(re2_search(c0, c1))", input);
    assertEqualVectors(makeNullConstant(TypeKind::BOOLEAN, 3), result);

    // At least one non null result.
    result = evaluate("try(re2_search(c0, c1))", oneGoodInput);
    assertEqualVectors(
        makeNullableFlatVector<bool>({std::nullopt, true, std::nullopt}),
        result);
  }

  // Ensure like works well with Try.
  {
    auto input = makeRowVector({stringVector});
    VELOX_ASSERT_THROW(
        evaluate("c0 like 'test_o' escape 'o'", input),
        "Escape character must be followed by '%', '_' or the escape character itself");

    auto result = evaluate("try(c0 like 'test_o' escape 'o')", input);
    assertEqualVectors(makeNullConstant(TypeKind::BOOLEAN, 3), result);

    VELOX_ASSERT_THROW(
        evaluate("c0 like 'test' escape 'oops'", input),
        "Escape string must be a single character");

    result = evaluate("try(c0 like 'test' escape 'oops')", input);
    assertEqualVectors(makeNullConstant(TypeKind::BOOLEAN, 3), result);
  }

  // Try bad index's.
  {
    VELOX_ASSERT_THROW(
        evaluateOnce<std::string>(
            "re2_extract_all(c0, c1, c2)",
            std::optional("123"),
            std::optional("abc"),
            std::optional(100)),
        "No group 100 in regex 'abc'");

    auto input = makeRowVector(
        {stringVector, makeConstant("abc", 3), makeConstant(100, 3)});
    auto result = evaluate("try(re2_extract(c0, c1, c2))", input);

    assertEqualVectors(makeNullConstant(TypeKind::VARCHAR, 3), result);

    // At least one non null result.
    input = makeRowVector(
        {makeConstant("123a 2b ", 3),
         makeConstant("(\\d+)([a-z]+)", 3),
         makeNullableFlatVector<int64_t>({std::nullopt, 1, 100})});
    assertEqualVectors(
        makeNullableArrayVector<StringView>(
            {std::nullopt, {{"123"_sv, "2"_sv}}, std::nullopt}),
        evaluate("try(re2_extract_all(c0, c1, c2))", input));
  }
}

// Make sure we do not compile more than kMaxCompiledRegexes.
TEST_F(Re2FunctionsTest, likeRegexLimit) {
  VectorPtr pattern = makeFlatVector<StringView>(26);
  VectorPtr input = makeFlatVector<StringView>(26);
  VectorPtr result;

  auto flatInput = input->asFlatVector<StringView>();
  for (int i = 0; i < 26; i++) {
    flatInput->set(i, "");
  }

  auto flatPattern = pattern->asFlatVector<StringView>();
  auto getPatternAtIdx = [&](PatternKind patternKind,
                             vector_size_t idx) -> std::string {
    switch (patternKind) {
      case PatternKind::kExactlyN:
        return fmt::format("{:_<{}}", "", idx + 1);
      case PatternKind::kAtLeastN:
        return fmt::format("{:%<{}}", "", idx + 1);
      case PatternKind::kFixed:
        return fmt::format("abc{}", idx);
      case PatternKind::kPrefix:
        return fmt::format("abc{}%%", idx);
      case PatternKind::kSuffix:
        return fmt::format("%%{}abc", idx);
      case PatternKind::kSubstring:
        return fmt::format("%%abc{}%%%", idx);
      default:
        VELOX_UNREACHABLE("like is not optimized for pattern {}", patternKind);
    }
  };

  auto verifyNoRegexCompilationForPattern = [&](PatternKind patternKind) {
    // Over 20 all optimized, will pass.
    for (int i = 0; i < 26; i++) {
      std::string patternAtIdx = getPatternAtIdx(patternKind, i);
      flatPattern->set(i, StringView(patternAtIdx));
    }
    result = evaluate("like(c0 , c1)", makeRowVector({input, pattern}));
    // Pattern '%%%', of type kAtleastN, matches with empty input.
    assertEqualVectors(
        makeConstant((patternKind == PatternKind::kAtLeastN), 26), result);
  };

  // Infer regex compilation does not happen for optimized patterns by verifying
  // less than kMaxCompiledRegexes are compiled for each optimized pattern type.
  verifyNoRegexCompilationForPattern(PatternKind::kExactlyN);
  verifyNoRegexCompilationForPattern(PatternKind::kAtLeastN);
  verifyNoRegexCompilationForPattern(PatternKind::kFixed);
  verifyNoRegexCompilationForPattern(PatternKind::kPrefix);
  verifyNoRegexCompilationForPattern(PatternKind::kSuffix);
  verifyNoRegexCompilationForPattern(PatternKind::kSubstring);

  // Over 20, all require regex, will fail.
  for (int i = 0; i < 26; i++) {
    std::string localPattern =
        fmt::format("b%[0-9]+.*{}.*{}.*[0-9]+", 'c' + i, 'c' + i);
    flatPattern->set(i, StringView(localPattern));
  }

  VELOX_ASSERT_THROW(
      evaluate("like(c0 , c1)", makeRowVector({input, pattern})),
      "Max number of regex reached");

  // Make sure try does not suppress that faluire.
  VELOX_ASSERT_THROW(
      evaluate("try(like(c0 , c1))", makeRowVector({input, pattern})),
      "Max number of regex reached");

  // All are complex but the same, should pass.
  for (int i = 0; i < 26; i++) {
    flatPattern->set(i, "b%[0-9]+.*{}.*{}.*[0-9]+");
  }
  assertEqualVectors(
      BaseVector::createConstant(BOOLEAN(), false, 26, pool()), result);
}

TEST_F(Re2FunctionsTest, invalidEscapeChar) {
  VectorPtr pattern = makeFlatVector<StringView>({"A", "B", "C_%"});
  VectorPtr input = makeFlatVector<StringView>({"A", "B", "C"});
  VectorPtr escapeChar = makeFlatVector<StringView>({"AA", "", "C"});
  auto rowVector = makeRowVector({input, pattern, escapeChar});

  VELOX_ASSERT_THROW(
      evaluate("like(c0 , c1, c2)", rowVector),
      "Escape string must be a single character");

  VELOX_ASSERT_THROW(
      evaluate("like(c0 ,'AA', 'AA')", rowVector),
      "Escape string must be a single character");

  VELOX_ASSERT_THROW(
      evaluate("like(c0 ,'AA', '')", rowVector),
      "Escape string must be a single character");
  {
    auto result = evaluate("try(like(c0 , c1, c2))", rowVector);
    auto expected =
        makeNullableFlatVector<bool>({std::nullopt, std::nullopt, false});
    assertEqualVectors(expected, result);
  }

  {
    auto result = evaluate("try(like(c0 , 'AA', 'AA'))", rowVector);
    assertEqualVectors(makeNullConstant(TypeKind::BOOLEAN, 3), result);
  }
}

} // namespace
} // namespace facebook::velox::functions
