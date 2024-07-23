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

list = ["apple", "banana", "orange", "mango", "strawberries", "blueberries", "strawberries", "apple", "apple", "apple"]
count = length(list)

def setToMax(file_path) do
  if count >= 5000 
    count = 5000
end

def split(file_path) do
  chunks_of_5 = Enum.chunk_every(list, 5)
  Enum.map(chunks, fn chunk ->
      length = div(length(chunk), 5)
      Enum.chunk_every(chunk, length)
    end)
end



def remove_special_chars(file_path) do
  file_path
  File.read!()
  String.replace(".|!|?|,", "")
  File.write!(file_path)
end

def to_lowercase(file_path) do
  file_path
  File.read!()
  String.downcase()
  File.write!(file_path)
end
