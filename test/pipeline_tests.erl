-module(pipeline_tests).
-include_lib("eunit/include/eunit.hrl").

pipeline_test() ->
    process_flag(trap_exit, true),
    F1 = fun()    -> {ok,    <<"Hello_1 ">>} end,
    F2 = fun(Msg) -> {error, Msg}            end,
    F3 = fun()    -> {ok,    [<<"Done">>]}   end,
    F4 = fun(Msg1, Msg2) -> {ok, [[list_to_binary(Msg1 ++ Msg2)]]} end,
    F5 = fun()    -> {ok,    "All right"} end,
    Spec = [
            {F1, []},
            {F2, [ignore]},
            {F3, [{args, []}]},
            {F4, [{args, ["Test", " Done!"]}]},
            {io, format, [{args_l, ["Message: ~p\n"]}]},
            {F5, []}
           ],
    ?assertMatch({ok, _Pid}, pipeline:start_link({local, pipeline_test}, Spec)),
    ?assertEqual({ok, "All right"}, pipeline:go(pipeline_test)).
