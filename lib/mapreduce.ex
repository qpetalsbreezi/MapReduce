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

  def solve(mapper_func, _reducer_func, data \\ from_file() ) do
    string_collections =
      Stream.map(data, fn line -> String.split(line, " ") end)
      |> Stream.map(fn strings -> remove_special_chars(strings) end)
      |> Stream.map(fn strings -> to_lowercase(strings) end)

      string_collections |> Enum.into([])

    intermediary_key_values = schedule(string_collections |> Enum.into([]), mapper_func)
    IO.puts("intermediary_key_values: ")
    IO.inspect(intermediary_key_values)

    schedule_reduce(intermediary_key_values)
  end


  def worker() do
    receive do
      {{data, lambda_func}, pid} ->
        lambda_result = lambda_func.(data)
        IO.inspect(data, label: "grouped data")
        IO.inspect(lambda_result, label: "recuder result")
        send(pid, {:result, lambda_result})
        worker()
    end
  end

  def schedule_reduce(intermediary_key_values) do
    scheduler_pid = self()
    reducer_scheduler_loop = spawn(fn -> reducer_scheduler_loop(%{responses: [], pending: length(intermediary_key_values)}, scheduler_pid) end)
    worker_count = 5

    workers =
      Enum.map(0..worker_count-1, fn worker_id ->
        {worker_id, spawn(fn -> worker() end)}
      end)

    intermediary_key_values
    |> List.flatten
    |> Enum.group_by(fn {key, _value} -> key end)
    |> Enum.map(fn {key, grouped_by_set} ->
      index = :crypto.hash(:sha, key)
              |> Base.encode16()
              |> Integer.parse(16)
              |> elem(0)
              |> Integer.mod(worker_count)

      worker = Enum.find(workers, fn {worker_id, _worker_pid} -> worker_id == index end)
      {_, worker_pid} = worker
      send(worker_pid, {{grouped_by_set, &sample_reducer/1}, reducer_scheduler_loop})
      end)

    receive do
      final_result -> IO.inspect(final_result, label: "result of reducer phase")
    end
  end

  def reducer_scheduler_loop(%{responses: responses, pending: pending}, caller_pid) do
    if pending == 0 do
      send(caller_pid, responses)
    end

    receive do
      {:result, result} ->
        new_state = %{responses: [result | responses], pending: pending - 1}
        IO.puts("new_state is:")
        IO.inspect(new_state)

        reducer_scheduler_loop(new_state, caller_pid)
    end
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

  def sample_reducer(grouped_key_values) do
    Enum.reduce(grouped_key_values, %{}, fn {key, value}, acc ->
      Map.update(acc, key, value, fn existing_value -> existing_value + value end)
    end )
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
