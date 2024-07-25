defmodule Mapreduce do
  @moduledoc """
  Documentation for `Mapreduce`.
  """

  def from_file(file_path \\ "sample.txt") do
    stream = File.stream!(file_path)
    stream
  end

  def sample_solve() do
    solve(&sample_mapper/1, nil)
  end

  def solve(mapper_func, reducer_func, data \\ from_file() ) do
    string_collections =
      Stream.map(data, fn line -> String.split(line, " ") end)
      |> Stream.map(fn strings -> remove_special_chars(strings) end)
      |> Stream.map(fn strings -> to_lowercase(strings) end)

      string_collections |> Enum.into([])

    final_result = schedule(string_collections |> Enum.into([]), mapper_func)
    IO.puts("final result is: ")
    IO.inspect(final_result)

    Enum.map(final_result, fn result ->
      Enum.map(result, fn {key, value} ->
        index = :crypto.hash(:sha, key) |> Base.encode16() |> Integer.parse() |> elem(0) |> Integer.mod(5)

        # TODO: fix me
        {:ok, file} = File.open("intermediary/#{index}.txt", [:append])
        IO.puts("opened file.")
        IO.inspect(file)
        IO.binwrite(file, {key, value})
      end)
    end)

    # TODO: Go through the responses, assign them to reducers
  end

  def schedule(string_collections, mapper_func) do
    scheduler_pid = self()
    scheduler_loop_pid = spawn(fn -> scheduler_loop(%{responses: [], pending: length(string_collections)}, scheduler_pid) end)
      Enum.map(string_collections, fn strings ->
        _pid = spawn(fn ->
          result = mapper_func.(strings)
          send(scheduler_loop_pid, {:result, result})
        end)
      end)

      receive do
        final_result -> final_result
      end
  end

  def scheduler_loop(%{responses: responses, pending: pending}, caller_pid) do
    if pending == 0 do
      send(caller_pid, responses)
    end

    receive do
      {:result, result} ->
        new_state = %{responses: [result | responses], pending: pending - 1}
        IO.puts("new_state is:")
        IO.inspect(new_state)

        scheduler_loop(new_state, caller_pid)
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
