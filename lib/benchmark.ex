defmodule Benchmark do
  def run() do
    small_file = "sample.txt"
    medium_file = "sample2.txt"
    large_file = "sample3.txt"

    Benchee.run(
      %{
        "Small file" => fn -> Mapreduce.solve_word_count(small_file) end,
        "Medium file" => fn -> Mapreduce.solve_word_count(medium_file) end,
        "Large file" => fn -> Mapreduce.solve_word_count(large_file) end
      }
    )
  end

end
