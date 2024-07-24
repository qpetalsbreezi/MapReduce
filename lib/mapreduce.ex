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

  def solve(mapper_func, reducer_func, data \\ from_file() ) do
    string_collections =
      Stream.map(data, fn line -> String.split(line, " ") end)
      |> Stream.map(fn strings -> remove_special_chars(strings) end)
      |> Stream.map(fn strings -> to_lowercase(strings) end)

      string_collections |> Enum.into([]) |> IO.inspect()

    scheduler_pid = Process.spawn(fn -> schedule() end)
    send({:run, string_collections, mapper_func})

    # Stream.map(string_collections, mapper_func)
    # |> Enum.into([])
  end

  def schedule(state \\ %{}) do
    scheduler_pid = self()
    receive do
      {:run, string_collections, mapper_func} ->
        Enum.map(string_collections, fn strings ->
          # TODO:
          # create a process.
          # run sample_mapper inside the process
          # send a messsage back to scheduler_pid containing the result
        end)
        schedule(state)
      {:result, result} ->
        nil
        # TODO: add this to the state. Call the function again with the new state
    end

  end

  @spec sample_mapper(any()) :: list()
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
      |> String.replace(~r/[.!?,\n]/, "") # needs to be fixed
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
