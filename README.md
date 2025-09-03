# sqlerrcat
This project analyses SQL statements and labels possible errors or complications.

# SQL Misconceptions TODO List
| ID    | Category  | Name                                  | Description                                                           | Base Query | Subquery | CTE |  
| :---: | :-------: | :------------------------------------ | --------------------------------------------------------------------- | :-: | :-: | :-: |
| 1     | SYN-1     | Ambiguous database object             | Omitting correlation names                                            | [x] | [ ] | [ ] |
| 2     | SYN-1     | Ambiguous database object             | Ambiguous column                                                      | [x] | [ ] | [ ] |
| 3     | SYN-1     | Ambiguous database object             | Ambiguous function                                                    | [ ] | [ ] | [ ] |
| 4     | SYN-2     | Undefined database object             | Undefined column                                                      | [x] | [x] | [x] |
| 5     | SYN-2     | Undefined database object             | Undefined function                                                    | [x] | [x] | [x] |
| 6     | SYN-2     | Undefined database object             | Undefined parameter                                                   | [x] | [x] | [x] |
| 7     | SYN-2     | Undefined database object             | Undefined object                                                      | [x] | [x] | [x] |
| 8     | SYN-2     | Undefined database object             | Invalid schema name                                                   | [x] | [ ] | [ ] |
| 9     | SYN-2     | Undefined database object             | Misspellings                                                          | [x] | [ ] | [ ] |
| 10    | SYN-2     | Undefined database object             | Synonyms                                                              | [ ] | [ ] | [ ] |
| 11    | SYN-2     | Undefined database object             | Omitting quotes around character data                                 | [x] | [x] | [x] |
| 12    | SYN-3     | Data type mismatch                    | Failure to specify column name twice                                  | [ ] | [ ] | [ ] |
| 13    | SYN-3     | Data type mismatch                    | Data type mismatch                                                    | [x] | [ ] | [ ] |
| 14    | SYN-4     | Illegal aggregate function placement  | Using aggregate function outside SELECT or HAVING                     | [x] | [x] | [x] |
| 15    | SYN-4     | Illegal aggregate function placement  | Grouping error: aggregate functions cannot be nested                  | [x] | [x] | [x] |
| 16    | SYN-5     | Illegal or insufficient grouping      | Grouping error: extraneous or omitted grouping column                 | [x] | [x] | [x] |
| 17    | SYN-5     | Illegal or insufficient grouping      | Strange HAVING: HAVING without GROUP BY                               | [x] | [x] | [x] |
| 18    | SYN-6     | Common syntax error                   | Confusing function with function                                      | [ ] | [ ] | [ ] |
| 19    | SYN-6     | Common syntax error                   | Using WHERE twice                                                     | [x] | [x] | [x] |
| 20    | SYN-6     | Common syntax error                   | Omitting the FROM clause                                              | [x] | [x] | [x] |
| 21    | SYN-6     | Common syntax error                   | Comparison with NULL                                                  | [x] | [x] | [x] |
| 22    | SYN-6     | Common syntax error                   | Omitting the semicolon                                                | [x] | [x] | [x] |
| 23    | SYN-6     | Common syntax error                   | Date time field overflow                                              | [ ] | [ ] | [ ] |
| 24    | SYN-6     | Common syntax error                   | Duplicate clause                                                      | [ ] | [ ] | [ ] |
| 25    | SYN-6     | Common syntax error                   | Using an undefined correlation name                                   | [ ] | [ ] | [ ] |
| 26    | SYN-6     | Common syntax error                   | Too many columns in subquery                                          | [ ] | [ ] | [ ] |
| 27    | SYN-6     | Common syntax error                   | Confusing table names with column names                               | [ ] | [ ] | [ ] |
| 28    | SYN-6     | Common syntax error                   | Restriction in SELECT clause (e.g., SELECT fee > 10)                  | [x] | [x] | [x] |
| 29    | SYN-6     | Common syntax error                   | Projection in WHERE clause (e.g., WHERE firstname, surname)           | [x] | [x] | [x] |
| 30    | SYN-6     | Common syntax error                   | Confusing the order of keywords (e.g., FROM customer SELECT fee)      | [x] | [ ] | [ ] |
| 31    | SYN-6     | Common syntax error                   | Confusing the logic of keywords (e.g., grouping instead of ordering)  | [ ] | [ ] | [ ] |
| 32    | SYN-6     | Common syntax error                   | Confusing the syntax of keywords (e.g., LIKE (‘A’, ‘B’))              | [x] | [x] | [x] |
| 33    | SYN-6     | Common syntax error                   | Omitting commas                                                       | [x] | [x] | [x] |
| 34    | SYN-6     | Common syntax error                   | Curly, square or unmatched brackets                                   | [x] | [x] | [x] |
| 35    | SYN-6     | Common syntax error                   | IS where not applicable                                               | [ ] | [ ] | [ ] |
| 36    | SYN-6     | Common syntax error                   | Nonstandard keywords or standard keywords in wrong context            | [ ] | [ ] | [ ] |
| 37    | SYN-6     | Common syntax error                   | Nonstandard operators (e.g., &&, || or ==)                            | [x] | [x] | [x] |
| 38    | SYN-6     | Common syntax error                   | Additional semicolon                                                  | [x] | [x] | [x] |
| 39    | SEM-1     | Inconsistent expression               | AND instead of OR (empty result table)                                | [x] | [ ] | [ ] |
| 40    | SEM-1     | Inconsistent expression               | Implied, tautological or inconsistent expression                      | [ ] | [ ] | [ ] |
| 41    | SEM-1     | Inconsistent expression               | DISTINCT in SUM or AVG                                                | [x] | [x] | [x] |
| 42    | SEM-1     | Inconsistent expression               | DISTINCT that might remove important duplicates                       | [ ] | [ ] | [ ] |
| 43    | SEM-1     | Inconsistent expression               | Wildcards without LIKE                                                | [x] | [x] | [x] |
| 44    | SEM-1     | Inconsistent expression               | Incorrect wildcard: using _ instead of % or using, e.g., *            | [x] | [x] | [x] |
| 45    | SEM-1     | Inconsistent expression               | Mixing a > 0 with IS NOT NULL or empty string with NULL               | [x] | [x] | [x] |
| 46    | SEM-2     | Inconsistent join                     | NULL in IN/ANY/ALL subquery                                           | [ ] | [ ] | [ ] |
| 47    | SEM-2     | Inconsistent join                     | Join on incorrect column (matches impossible)                         | [ ] | [ ] | [ ] |
| 48    | SEM-3     | Missing join                          | Omitting a join                                                       | [ ] | [ ] | [ ] |
| 49    | SEM-4     | Duplicate rows                        | Many duplicates                                                       | [ ] | [ ] | [ ] |
| 50    | SEM-5     | Redundant column output               | Constant column output                                                | [x] | [ ] | [ ] |
| 51    | SEM-5     | Redundant column output               | Duplicate column output                                               | [x] | [ ] | [ ] |
| 52    | LOG-1     | Operator error                        | OR instead of AND                                                     | [x] | [x] | [x] |
| 53    | LOG-1     | Operator error                        | Extraneous NOT operator                                               | [ ] | [ ] | [ ] |
| 54    | LOG-1     | Operator error                        | Missing NOT operator                                                  | [ ] | [ ] | [ ] |
| 55    | LOG-1     | Operator error                        | Substituting existence negation with <>                               | [ ] | [ ] | [ ] |
| 56    | LOG-1     | Operator error                        | Putting NOT in front of incorrect IN/EXISTS                           | [ ] | [ ] | [ ] |
| 57    | LOG-1     | Operator error                        | Incorrect comparison operator or incorrect value compared             | [x] | [x] | [x] |
| 58    | LOG-2     | Join error                            | Join on incorrect table                                               | [ ] | [ ] | [ ] |
| 59    | LOG-2     | Join error                            | Join when join needs to be omitted                                    | [ ] | [ ] | [ ] |
| 60    | LOG-2     | Join error                            | Join on incorrect column (matches possible)                           | [ ] | [ ] | [ ] |
| 61    | LOG-2     | Join error                            | Join with incorrect comparison operator                               | [ ] | [ ] | [ ] |
| 62    | LOG-2     | Join error                            | Missing join                                                          | [ ] | [ ] | [ ] |
| 63    | LOG-3     | Nesting error                         | Improper nesting of expressions                                       | [ ] | [ ] | [ ] |
| 64    | LOG-3     | Nesting error                         | Improper nesting of subqueries                                        | [ ] | [ ] | [ ] |
| 65    | LOG-4     | Expression error                      | Extraneous quotes                                                     | [ ] | [ ] | [ ] |
| 66    | LOG-4     | Expression error                      | Missing expression                                                    | [x] | [x] | [x] |
| 67    | LOG-4     | Expression error                      | Expression on incorrect column                                        | [x] | [x] | [x] |
| 68    | LOG-4     | Expression error                      | Extraneous expression                                                 | [x] | [x] | [x] |
| 69    | LOG-4     | Expression error                      | Expression in incorrect clause                                        | [ ] | [ ] | [ ] |
| 70    | LOG-5     | Projection error                      | Extraneous column in SELECT                                           | [x] | [x] | [x] |
| 71    | LOG-5     | Projection error                      | Missing column from SELECT                                            | [x] | [x] | [x] |
| 72    | LOG-5     | Projection error                      | Missing DISTINCT from SELECT                                          | [ ] | [ ] | [ ] |
| 73    | LOG-5     | Projection error                      | Missing AS from SELECT                                                | [ ] | [ ] | [ ] |
| 74    | LOG-5     | Projection error                      | Missing column from ORDER BY clause                                   | [x] | [ ] | [ ] |
| 75    | LOG-5     | Projection error                      | Incorrect column in ORDER BY clause                                   | [x] | [ ] | [ ] |
| 76    | LOG-5     | Projection error                      | Extraneous ORDER BY clause                                            | [x] | [ ] | [ ] |
| 77    | LOG-5     | Projection error                      | Incorrect ordering of rows                                            | [x] | [ ] | [ ] |
| 78    | LOG-6     | Function error                        | DISTINCT as function parameter where not applicable                   | [ ] | [ ] | [ ] |
| 79    | LOG-6     | Function error                        | Missing DISTINCT from function parameter                              | [ ] | [ ] | [ ] |
| 80    | LOG-6     | Function error                        | Incorrect function                                                    | [ ] | [ ] | [ ] |
| 81    | LOG-6     | Function error                        | Incorrect column as function parameter                                | [ ] | [ ] | [ ] |
| 82    | COM       | Complication                          | Unnecessary complication                                              | [ ] | [ ] | [ ] |
| 83    | COM       | Complication                          | Unnecessary DISTINCT in SELECT clause                                 | [x] | [x] | [x] |
| 84    | COM       | Complication                          | Unnecessary join                                                      | [x] | [ ] | [ ] |
| 85    | COM       | Complication                          | Unused correlation name                                               | [ ] | [ ] | [ ] |
| 86    | COM       | Complication                          | Correlation names are always identical                                | [ ] | [ ] | [ ] |
| 87    | COM       | Complication                          | Unnecessarily general comparison operator                             | [ ] | [ ] | [ ] |
| 88    | COM       | Complication                          | LIKE without wildcards                                                | [x] | [x] | [x] |
| 89    | COM       | Complication                          | Unnecessarily complicated SELECT in EXISTS subquery                   | [ ] | [ ] | [ ] |
| 90    | COM       | Complication                          | IN/EXISTS can be replaced by comparison                               | [ ] | [ ] | [ ] |
| 91    | COM       | Complication                          | Unnecessary aggregate function                                        | [ ] | [ ] | [ ] |
| 92    | COM       | Complication                          | Unnecessary DISTINCT in aggregate function                            | [ ] | [ ] | [ ] |
| 93    | COM       | Complication                          | Unnecessary argument of COUNT                                         | [ ] | [ ] | [ ] |
| 94    | COM       | Complication                          | Unnecessary GROUP BY in EXISTS subquery                               | [ ] | [ ] | [ ] |
| 95    | COM       | Complication                          | GROUP BY with singleton groups                                        | [ ] | [ ] | [ ] |
| 96    | COM       | Complication                          | GROUP BY with only a single group                                     | [ ] | [ ] | [ ] |
| 97    | COM       | Complication                          | GROUP BY can be replaced with DISTINCT                                | [ ] | [ ] | [ ] |
| 98    | COM       | Complication                          | UNION can be replaced by OR                                           | [ ] | [ ] | [ ] |
| 99    | COM       | Complication                          | Unnecessary column in ORDER BY clause                                 | [x] | [ ] | [ ] |
| 100   | COM       | Complication                          | ORDER BY in subquery                                                  | [ ] | [ ] | [ ] |
| 101   | COM       | Complication                          | Inefficient HAVING                                                    | [ ] | [ ] | [ ] |
| 102   | COM       | Complication                          | Inefficient UNION                                                     | [ ] | [ ] | [ ] |
| 103   | COM       | Complication                          | Condition in the subquery can be moved up                             | [ ] | [ ] | [ ] |
| 104   | COM       | Complication                          | Condition on left table in LEFT OUTER JOIN                            | [ ] | [ ] | [ ] |
| 105   | COM       | Complication                          | OUTER JOIN can be replaced by INNER JOIN                              | [ ] | [ ] | [ ] |
