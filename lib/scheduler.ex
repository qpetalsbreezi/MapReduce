defmodule Scheduler do
  use GenServer

  def start_link() do
    {:ok, _pid} = GenServer.start_link(__MODULE__, [])
  end

  @impl true
  def init(_args) do
    {:ok, %{}}
  end

  @spec schedule(atom() | pid() | {atom(), any()} | {:via, atom(), any()}, any(), any()) :: any()
  def schedule(scheduler_pid, string_collections, mapper_func) do
    GenServer.call(scheduler_pid, {:schedule, {string_collections, mapper_func}})
    receive do
      response -> response
     end
  end

  def worker() do
    receive do
      {{data, lambda_func}, scheduler_pid, caller_pid} ->

        lambda_result = lambda_func.(data)
        GenServer.cast(scheduler_pid, {{:result, lambda_result}, caller_pid})
        worker()
    end
  end

  @impl true
  def handle_call({:schedule, {string_collections, mapper_func}}, from, _state) do
    pid = self()
    initial_state = %{responses: [], pending: length(string_collections), mapper_func: mapper_func}

    # IO.inspect("debug: length of string_collections is #{length(string_collections)}")
    worker_count = 5
    workers =
      Enum.map(0..worker_count-1, fn worker_id ->
        {worker_id, spawn(fn -> worker() end)}
      end)

    Enum.with_index(string_collections, fn strings, index ->
      # IO.inspect("calculating result for line number: #{index}")
      {_, worker} = Enum.find(workers, fn {worker_id, _worker_pid} -> worker_id == index |> Integer.mod(worker_count) end)
      send(worker, {{strings, mapper_func}, self(), from |> elem(0)})
    end)

    {:reply, initial_state, initial_state}
  end


  @impl true
  def handle_cast({{:result, result}, caller_pid}, %{responses: responses, pending: pending} = state) do
    new_state = %{state | responses: [result | responses], pending: pending - 1}

    # IO.inspect("got result. counter is: #{new_state.pending}")
    if new_state.pending == 0 do
      send(caller_pid, new_state.responses)
    end

    {:noreply, new_state}
  end
end
