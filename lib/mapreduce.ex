defmodule Mapreduce do
  @moduledoc """
  Documentation for `Mapreduce`.
  """

  @doc """
  Hello world

  ## Examples

      iex> Mapreduce.hello()
      :world

  """
  def hello do
    :world
  end

  def from_file(file_path \\ "sample.txt") do
    stream = File.stream!(file_path)
    stream
  end

  def solve(mapper_func, reducer_func, data \\ from_file ) do
    # files_dump =
    #   Stream.map(file_path_collection, fn file_path ->
    #     # take the file path. convert it to the data the file has
    #     from_file(file_path)
    #   end)

    collection_of_strings =
      Stream.map(files_dump, fn str -> String.split(str, " ") end)
      |> Enum.map(fn strings -> remove_special_chars(strings) end)
      |> Enum.map(fn strings -> to_lowercase(strings) end)

    Enum.map(collection_of_strings, mapper_func)
  end

  def sample_mapper(string_collection) do
    Enum.map(string_collection, fn str -> {str, 1} end)
  end

  list = [
    "apple",
    "banana",
    "orange",
    "mango",
    "strawberries",
    "blueberries",
    "strawberries",
    "apple",
    "apple",
    "apple"
  ]

  count = length(list)

  # def setToMax(file_path) do
  #   if(count >= 5000)
  #   count = 5000
  # end

  # def split(file_path) do
  #   chunks_of_5 = Enum.chunk_every(list, 5)

  #   Enum.map(chunks, fn chunk ->
  #     length = div(length(chunk), 5)
  #     Enum.chunk_every(chunk, length)
  #   end)
  # end

  def remove_special_chars(strings) do
    strings
    |> Enum.map(fn string ->
      string
      |> String.replace(~r/[.!?,]/, "") # needs to be fixed
    end)
  end

  def to_lowercase(strings) do
    strings
    |> Enum.map(fn string ->
      string
      |> String.downcase()
    end)
  end
end
