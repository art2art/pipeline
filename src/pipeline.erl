-module(pipeline).
-author('Artem Artemiev <art.art.v@gmail.com>').

-behaviour(gen_server).

-export([start_link/1, start_link/2]).
-export([go/1, go/2, test/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-type pipe_out()  :: {ok, term()} | {error, term()}.
-type pipe_fun()  :: fun(() -> pipe_out())
                   | fun((Args :: term()) -> pipe_out()).
-type pipe_name() :: pid()
                   | atom()
                   | {'local', Name :: atom()}
                   | {'global', Name :: atom()}.
-type pipe_opt()  :: {'timeout', Timeout :: timeout()}
                   | {'args', Args :: [term()]}
                   | {'args_r', Args :: [term()]}
                   | {'args_l', Args :: [term()]}
                   | 'ignore'.
-type pipe_opts() :: [pipe_opt()].
-type pipe_spec() :: {Module :: atom(), Function :: atom(), Opts :: pipe_opts()}
                   | {Module :: atom(), Opts :: pipe_opts()}
                   | {Pid    :: pid(), Opts :: pipe_opts()}
                   | {Fun    :: fun((...) -> pipe_out()), Opts :: pipe_opts()}.

-callback exec(Arg :: term()) -> pipe_out().

-record(state, {acts = [] :: [pipe_fun()]}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link(PipeName, Specs) -> {'ok', pid()} when
      PipeName :: pipe_name(),
      Specs :: [pipe_spec()].
start_link(PipeName, Specs) ->
    gen_server:start_link(PipeName, ?MODULE, Specs, []).

-spec start_link(Specs :: [pipe_spec()]) -> {'ok', pid()}.
start_link(Specs) ->
    gen_server:start_link(?MODULE, Specs, []).

-spec go(RefPipe) -> {'ok', term()} | {'error', Reason} when
      RefPipe :: pipe_name(),
      Reason :: term().
go(RefPipe) ->
    gen_server:call(RefPipe, exec).

-spec go(RefPipe, Initial) -> {'ok', term()} | {'error', Reason} when
      RefPipe :: pipe_name(),
      Initial :: term(),
      Reason :: term().
go(RefPipe, Arg) ->
    gen_server:call(RefPipe, {exec, Arg}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(Specs) ->
    %% erlang:process_flag(trap_exit, true),
    timer:apply_after(0, gen_server, cast, [self(), {init, Specs}]),
    {ok, #state{}}.

handle_call(exec, From, State) ->
    handle_call({exec, []}, From, State);
handle_call({exec, Initial}, _From, State) ->
    case exec(State#state.acts, Initial) of
        {error, _, _} = Error ->
            {stop, brutal_kill, Error, State};
        [] ->
            {stop, shutdown, ok, State};
        {ok, _} = Result0 ->
            {stop, shutdown, Result0, State};
        Result1 ->
            {stop, shutdown, {ok, Result1}, State}
    end.

handle_cast({init, Specs}, State) ->
    ok = check_specs(Specs),
    Acts = build_acts(Specs),
    {noreply, State#state{acts = Acts}}.

handle_info(_Msg, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
terminate(_Reason, _State) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(EXECUTE(Fun, Arg),
        case is_function(Fun, 0) of
            true  -> Fun();
            false -> Fun(Args)
        end).

-spec exec(Funs :: [pipe_fun()], Args :: [term()]) -> term() | {error, term()}.
exec([], Result) ->
    Result;
exec([Fun | Rest], Args) ->
    case ?EXECUTE(Fun, Args) of
        {error, Reason} ->
            report_error(Reason),
            {error, Reason, {Args, Rest}};
        Result when is_list(Result) ->
            exec(Rest, Result);
        Result ->
            exec(Rest, [Result])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec chopts(Opts :: [term()]) -> 'ok' | {'bad_spec', Bad :: any()}.
chopts(Opts) when is_list(Opts) ->
    lists:foreach(fun cho/1, Opts).

cho({timeout, _}) -> ok;
cho({args,    _}) -> ok;
cho({args_r,  _}) -> ok;
cho({args_l,  _}) -> ok;
cho(ignore      ) -> ok;
cho(Other       ) -> throw({bad_spec, Other}).


-spec ch(Spec :: pipe_spec()) -> 'ok' | {'bad_spec', Bad :: any()}.
ch({Fun,    Opts}) when is_function(Fun)  -> chopts(Opts);
ch({Pid,    Opts}) when is_pid(Pid)       -> chopts(Opts);
ch({Module, Opts}) when is_atom(Module)   -> chopts(Opts);
ch({Module, Fun, Opts}) when is_atom(Module), is_atom(Fun) -> chopts(Opts);
ch(Other) -> throw({bad_spec, Other}).

-spec check_specs(Specs :: [pipe_spec()]) -> 'ok' | 'bad_spec'.
check_specs(Specs) ->
    case catch ([ch(S)|| S <- Specs]) of
        {bad_spec, _} = Bad -> Bad;
        _Other              -> ok
    end.

-spec build_acts(Specs :: [pipe_spec()]) -> [pipe_fun()].
build_acts(Specs) ->
    [build_act(S) || S <- Specs].

build_act({Fun, Opts}) when is_function(Fun) ->
    suit_fun(getval(args, Opts, undefined),
             getval(ignore, Opts, undefined),
             Fun);
build_act({Mod, Opts}) when is_atom(Mod) ->
    suit_fun(getval(args, Opts, undefined),
             getval(ignore, Opts, undefined),
             {Mod, exec});
build_act({Mod, Fun, Opts}) when is_atom(Mod), is_atom(Fun) ->
    suit_fun(getval(args, Opts, undefined),
             getval(ignore, Opts, undefined),
             {Mod, Fun});
build_act({Pid, Opts}) when is_pid(Pid) ->
    suit_fun(getval(args, Opts, undefined),
             getval(ignore, Opts, undefined),
             {Pid, getval(timeout, Opts, undefined)});
build_act({Port, Opts}) when is_port(Port) ->
    suit_fun(getval(args, Opts, undefined),
             getval(ignore, Opts, undefined),
             {Port, getval(timeout, Opts, undefined)}).

-define(HR(Res, Ign), handle_result(Res, Ign)).

suit_fun(undefined, Ignore, Type) ->
    case Type of
        Fun when is_function(Fun) ->
            fun(Args) -> ?HR(perform_fun(Fun, Args), Ignore) end;
        {Mod, Fun} when is_atom(Mod), is_atom(Fun) ->
            fun(Args) -> ?HR(perform_mfa(Mod, Fun, Args), Ignore) end;
        {Pid, Timeout} when is_pid(Pid) ->
            fun(Args) -> ?HR(perform_pid(Pid, Args, Timeout), Ignore) end;
        {Port, Timeout} when is_port(Port) ->
            fun(Args) -> ?HR(perform_port(Port, Args, Timeout), Ignore) end
    end;
suit_fun({args, Args}, Ignore, Type) ->
    case Type of
        Fun when is_function(Fun) ->
            fun() -> ?HR(perform_fun(Fun, Args), Ignore) end;
        {Mod, Fun} when is_atom(Mod), is_atom(Fun) ->
            fun() -> ?HR(perform_mfa(Mod, Fun, Args), Ignore) end;
        {Pid, Timeout} when is_pid(Pid) ->
            fun() -> ?HR(perform_pid(Pid, Args, Timeout), Ignore) end;
        {Port, Timeout} when is_port(Port) ->
            fun() -> ?HR(perform_port(Port, Args, Timeout), Ignore) end
    end;
suit_fun({args_l, SetArgs}, Ignore, Type) ->
    case Type of
        Fun when is_function(Fun) ->
            fun(InArgs) -> ?HR(perform_fun(Fun, SetArgs ++ InArgs), Ignore) end;
        {Mod, Fun} when is_atom(Mod), is_atom(Fun) ->
            fun(InArgs) -> ?HR(perform_mfa(Mod, Fun, SetArgs ++ InArgs), Ignore) end;
        {Pid, Timeout} when is_pid(Pid) ->
            fun(InArgs) -> ?HR(perform_pid(Pid, SetArgs ++ InArgs, Timeout), Ignore) end;
        {Port, Timeout} when is_port(Port) ->
            fun(InArgs) -> ?HR(perform_port(Port, SetArgs ++ InArgs, Timeout), Ignore) end
    end;
suit_fun({args_r, SetArgs}, Ignore, Type) ->
    case Type of
        Fun when is_function(Fun) ->
            fun(InArgs) -> ?HR(perform_fun(Fun, InArgs ++ SetArgs), Ignore) end;
        {Mod, Fun} when is_atom(Mod), is_atom(Fun) ->
            fun(InArgs) -> ?HR(perform_mfa(Mod, Fun, InArgs ++ SetArgs), Ignore) end;
        {Pid, Timeout} when is_pid(Pid) ->
            fun(InArgs) -> ?HR(perform_pid(Pid, InArgs ++ SetArgs, Timeout), Ignore) end;
        {Port, Timeout} when is_port(Port) ->
            fun(InArgs) -> ?HR(perform_port(Port, InArgs ++ SetArgs, Timeout), Ignore) end
    end.

perform_fun(Fun, Args) ->
    catch erlang:apply(Fun, Args).
perform_mfa(Mod, Fun, Args) ->
    catch erlang:apply(Mod, Fun, Args).
perform_pid(Pid, Msg, Timeout) ->               % TODO: should consider, must be completed.
    case erlang:is_process_alive(Pid) of
        false ->
            {error, {Pid, is_dead}};        true ->
            Ref = erlang:monitor(Pid),
            erlang:send(Pid, Msg),
            Rec = receive
                      {'DOWN', Ref, process, Pid, Reason} ->
                          {error, Reason};
                      Message ->
                          Message
                  after Timeout ->
                          {error, timeout}
                  end,
            erlang:demonitor(Ref),
            Rec
    end.
perform_port(Port, _Msg, _Timeout) ->           % TODO: should consider, must be completed.
    case erlang:port_info(Port) of
        'undefined' ->
            {error, {Port, is_not_connected}};
        _Other ->
            {error, todo}                       % TODO
    end.

handle_result(ok, _) -> [];
handle_result(_, ignore) -> [];
handle_result({ok, Result}, _) -> Result;
handle_result({error, Reason}, _) -> {error, Reason};
handle_result({'EXIT', Reason}, _) -> {error, Reason};
handle_result({'EXIT', _, Reason}, _) -> {error, Reason}.
%%
%% handle_result({'DOWN', _Ref, _, _Pid, Reason}, _Ignore) -> {error, Reason};

report_error(Reason) ->
    error_logger:error_report(pipeline, [{pid, self()}, {reason, Reason}]).

getval(args, List, Default) ->
    case lists:filter(fun({Key, _Value})
                            when Key =:= args; Key =:= args_l; Key =:= args_r
                                 -> true;
                         (_)     -> false
                      end, List)
    of
        []     -> Default;
        [Args] -> Args
    end;
getval(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        false ->
            case lists:member(Key, List) of
                false ->
                    Default;
                true ->
                    Key
            end;
        {_Key, Value} ->
            Value
    end.
