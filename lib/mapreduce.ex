defmodule Mapreduce do
  @moduledoc """
  Documentation for `Mapreduce`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Mapreduce.hello()
      :world

  """
  def hello do
    :world
  end

  def from_file(file_path \\ "sample.txt") do
    binary = File.read!(file_path)
    # IO.inspect(binary)

    binary
  end

  def solve(mapper_func, reducer_func, file_path_collection) do
    files_dump = Enum.map(file_path_collection, fn file_path ->
      # take the file path. convert it to the data the file has
      from_file(file_path)
    end)

    collection_of_strings = Enum.map(files_dump, fn str -> String.split(str, " ") end)


    Enum.map(collection_of_strings, mapper_func)
  end

  def sample_mapper(string_collection) do
    Enum.map(string_collection, fn str -> {str, 1} end)
  end
end
