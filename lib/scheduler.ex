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

  @impl true
  def handle_call({:schedule, {string_collections, mapper_func}}, from, _state) do
    pid = self()
    initial_state = %{responses: [], pending: length(string_collections), mapper_func: mapper_func}

    Enum.each(string_collections, fn strings ->
      spawn(fn ->
        result = mapper_func.(strings)
        GenServer.cast(pid, {{:result, result}, from |> elem(0)})
      end)
    end)

    {:reply, initial_state, initial_state}
  end


  @impl true
  def handle_cast({{:result, result}, caller_pid}, %{responses: responses, pending: pending} = state) do
    new_state = %{state | responses: [result | responses], pending: pending - 1}

    if new_state.pending == 0 do
      send(caller_pid, new_state.responses)
    end

    {:noreply, new_state}
  end
end
