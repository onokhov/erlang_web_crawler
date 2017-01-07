%%% @author Alexander Onokhov <ccnweb@gmail.com>
%%% @doc http crawler

-module(crawler).

-author("ccnweb@gmail.com").

-export([ crawler/1
        , crawler/2
        , worker/1
        ]).

%% @doc Загружает рекурсивно документы с сайта начиная с Url данного в параметрах.
%%  Параметры: Url начала загрузки и количество потоков.
crawler(Url) ->
    crawler(Url, 1).

%% вызываем рекурсивную процедуру загрузки
crawler(Url, NumWorkers) when NumWorkers >= 1 ->
    {ok,[_, Host, _, _, _]} = parse_url(string:to_lower(Url)),
    inets:start(), % запускаем службы httpc
    ssl:start(),
    FreeWorkers = lists:map(fun(_) -> spawn(crawler, worker, [self()]) end, lists:seq(1, NumWorkers)), % запускаем нужное количество воркеров
    erlang:send_after(10000, self(), save_state),    % через 10 секунд хотим сохранить состояние загрузки
    case file:consult(Host ++ ".state") of   % пытаемся восстановить состояние прерванной загрузки
        {ok, [[Host, UrlsRestored, SeenUrlsRestored]]} -> 
            io:format("Resume download session~n"),
            crawler([], UrlsRestored, FreeWorkers, SeenUrlsRestored, [], Host);
        _ -> 
            crawler([], [Url], FreeWorkers, [], [], Host)
    end.

%% @doc рекурсивная процедура загрузки
%% раздаём задания воркерам,
%% ожидаем сообщения от воркеров, в которых они передают новые задания для загрузки
%% ожидаем сообщение от таймера, по которому сохраняем текущее состояние загрузки в файл
crawler([], [], FreeWorkers, _SeenUrls, _UrlsInProgress, Host) ->  % паттерн завершения работы, всё загружено
    lists:foreach(fun(Pid) -> exit(Pid, stop) end, FreeWorkers),  % посылаем сигналы остановки воркерам
    file:delete(Host ++ ".state"), %  удаляем файл состояния загрузки
    ssl:stop(),  % останавливаем службы httpc
    inets:stop(),
    io:format("Crawler finished~n");
crawler(Workers, Urls, FreeWorkers, SeenUrls, UrlsInProgress, Host) when length(Urls) > 0, length(FreeWorkers) > 0 -> % паттерн раздачи заданий по воркерам
    [Url | UrlsLeft] = sets:to_list(sets:from_list(Urls)),
    [Worker | WorkersLeft] = FreeWorkers,
    Worker ! {url, Url},
    crawler( [Worker] ++ Workers, UrlsLeft, WorkersLeft, SeenUrls, [Url] ++ UrlsInProgress, Host); 
crawler(Workers, Urls, FreeWorkers, SeenUrls, UrlsInProgress, Host) -> % ждать сообщений, если нет свободных воркеров или нет свободных урлов
    receive
        save_state -> % это от таймера. Сохраним состояние и вернёмся в ожидание
            io:format("saving state...~n"),
            file:write_file(Host ++ ".state", io_lib:format("~p.~n", [[Host, lists:append(Urls, UrlsInProgress), SeenUrls]])),
            erlang:send_after(10000, self(), save_state),    % через 10 секунд снова хотим сохранить состояние загрузки
            crawler(Workers, Urls, FreeWorkers, SeenUrls, UrlsInProgress, Host);
        {worker, Pid, urls_to_fetch, NewUrls, url_fetched, Url} -> % воркер прислал новые урлы
            crawler( lists:delete(Pid,Workers), 
                     Urls ++ lists:filter(fun(U) ->     % фильтруем новые задания, не берем те, что уже сделаны или с других хостов
                                                  {ok, [_, HostToCheck, _, _, _]} = parse_url(U), 
                                                  IsForFetch = string:equal(HostToCheck, Host) 
                                                               andalso not lists:member(U, UrlsInProgress)
                                                               andalso not lists:member(U, SeenUrls),
                                                  IsForFetch
                                          end, NewUrls),
                     [Pid] ++ FreeWorkers, 
                     [Url] ++ SeenUrls, 
                     lists:delete(Url, UrlsInProgress),
                     Host)
    after 15000 ->
            io:format("Crawler timed out~n") % что-то пошло не так
    end.


%% @private
worker(Parent) -> 
    receive
        {url, Url} ->
%           io:format("url to fetch ~s~n",[Url]),
            Parent ! { worker, self(),
                       urls_to_fetch, fetch_and_save(Url),
                       url_fetched, Url
                     },
            worker(Parent)
    after 10000 ->
            io:format("worker ~w timed out. Parent ~w~n",[self(), Parent])
    end.

fetch_and_save(Url) ->
    %% асинхронный запрос, чтоб на этапе разбора заголовков можно было отказаться от загрузки нетекстовых документов
    io:format("fetch ~s~n",[Url]),
    R = httpc:request(get, {Url, []}, [], [{sync, false}, {stream, self}, {full_result, false}]),
    case R of
        {error, Reason} ->
            io:format("Error fetching ~s: ~w~n", [Url, Reason]),
            [];
        {ok, RequestId} ->
            {Html, UrlsToFetch} = parse_html(receive_text_data(RequestId), Url),
            save_to_file(Html, path_to_index(Url)),
            UrlsToFetch
    end.

receive_text_data(RequestId) ->
            receive_text_data(RequestId, []).

receive_text_data(RequestId, Accumulator) ->
    receive
        {http, {RequestId, stream_start, Headers}} ->
            case is_text_headers(Headers) of
                true ->
                    receive_text_data(RequestId);
                false ->
                    io:format("skip non text document~n"),
                    httpc:cancel_request(RequestId),
                    []
            end;
        {http, {RequestId, stream, BinBodyPart}} ->
            receive_text_data( RequestId, [BinBodyPart] ++ Accumulator );
        {http, {RequestId, stream_end, _Headers}} ->
            list_to_binary(lists:reverse(Accumulator))
    after 10000 ->
            io:format("receive timeout~n"),
            httpc:cancel_request(RequestId),
            []
    end.

is_text_headers(Headers) ->
    lists:any(fun({Header, Value}) -> 
                      case Header of
                          "content-type" ->
                              string:str(Value, "text/") == 1;
                          _ ->
                              false
                      end
              end, Headers).

%% @doc возвращает html с преобразованными ссылками из абсолютных в относительные и список ссылок для загрузки
extract_links([], _Html, ParsedParts, _Pos, _BaseUrl, Links) ->
            {lists:flatten(lists:reverse(ParsedParts)), Links};
extract_links([[{Start, Len}] | Positions], Html, ParsedParts, Pos, BaseUrl, Links) ->
    Link = binary_to_list(binary:part(Html, Start, Len)),
    extract_links( Positions,
                   Html,
                   [path_to_index(skip_query(url_to_relative(Link, BaseUrl))), binary_to_list(binary:part(Html, Start, Pos - Start))] ++ ParsedParts,
                   Start+Len,
                   BaseUrl,
                   [skip_query(url_to_absolute(Link, BaseUrl))] ++ Links
                 ).

parse_html(Html, BaseUrl) ->
    M = re:run(Html,
               <<"<(?:link|a|script)\\s+[^>]*(?:href|src)=(?|\"([^\"]+)\"|'([^']+)'|([^\\s><\"\']+))">>, 
               [dotall, global, caseless, {capture, all_but_first}]),
    case M of
        nomatch ->
            case Html of
                [] ->
                    {[], []};
                _ ->
                    {binary_to_list(Html), []}
            end;
        {match, CapturedPositions} ->
            extract_links(CapturedPositions, Html, [], 0, BaseUrl, [])
    end.

save_to_file(Html, Url) ->
%    io:format("save ~w bytes to ~s~n",[string:len(Html), url_to_filename(Url)]),
    case string:len(Html) of
        0 ->
            ok;
        _ ->
            Filename = url_to_filename(Url),
            filelib:ensure_dir(filename:dirname(Filename)++"/"),
%     io:format("f: ~s, d: ~s~n",[Filename, filename:dirname(Filename)++"/"]),
            file:write_file(Filename, Html)
    end.

url_to_filename(Url) ->
    Pos = string:str(Url, "://"),
    "." ++ string:substr(Url, Pos + 2).

path_to_index(Url) -> % если ссылка на каталог, то приписываем к ней index.html
    case string:right(Url,1) == "/" of
        true ->
            Url ++ "index.html";
        false ->
            Url
    end.

%%%
%%% ниже определены функции преобразования url и вспомогательные для преобразований
%%%

% @doc эту функцию используем вместо http_uri:parse потому как на не абсолютных урлах http_uri:parse падает
parse_url(Url) -> % {ok, [Scheme, Authority, Path, Query, Fragment]} | error
    case re:run(Url, "(?:([^:/?#]+):)?(?://([^/?#]*))?([^?#]*)(?:\\?([^#]*))?(?:#(.*))?", [{capture, all_but_first, list}]) of
        {match, Captured} ->
            {ok, lists:sublist(Captured, 3) ++ [[], []]}; % не работаем с query и fragment, для crawler`а они не нужны
        nomatch ->
            error
    end.

url_to_absolute(Url, BaseUrl) ->
    case re:run(Url,"^[a-zA-Z][a-zA-Z0-9.+-]*:") of
        {match, _} ->                        % url is absolute already
            normalize_url(Url);
        nomatch ->
            {ok, {Scheme, UserInfo, Host, Port, Path, _Query}} = http_uri:parse(BaseUrl),
            [UrlPath, UrlQuery] = case re:run(Url,"^([\\?]+)(\\?.*)",[{capture, all_but_first, list}]) of
                                       nomatch -> [Url,[]];
                                       {match, {Part1, Part2}} ->
                                          [Part1, Part2]
                                   end,
            case string:str(Url, "//") == 1 of
                true ->                     % url just has no scheme
                    scheme_to_string(Scheme) ++ ":" ++ Url;
                false ->
                    compose_url({Scheme, UserInfo, Host, Port, merge_paths(clean_path(Path), UrlPath), UrlQuery})
            end
    end.

url_to_relative(Url, BaseUrl) ->
%    io:format("to relative ~s, ~s~n",[Url, BaseUrl]),
    M = re:run(url_to_absolute(Url,BaseUrl),"^([^:]+://[^/]++)(.+)",[{capture, all_but_first, list}]),
    case M of
        nomatch ->
            Url;
        {match, [Left, Right]} ->
            {match, [BaseLeft, BaseRight]} = re:run(clean_path(BaseUrl), "^([^:]+://[^/]++)(.+)",[{capture, all_but_first, list}]),
            case string:equal(Left,BaseLeft) of
                false -> 
                    Url;
                true ->
                    Path = string:tokens(Right,"/"),
                    BasePath = string:tokens(BaseRight,"/"),
                    {PreparedP, PreparedB} = strip_common_head(Path, BasePath),
                    string:join(lists:append([["."], lists:map(fun(_) -> ".." end, PreparedB), PreparedP]),"/")
            end
    end.

strip_common_head([H|T1],[H|T2]) ->
    strip_common_head(T1,T2);
strip_common_head(A,B) ->
    {A,B}.

merge_paths(Base, Path) ->
    P = case string:sub_string(Path,1,1) of
            "/" ->
                Path;
            _ ->
                Base ++ "/" ++ Path
        end,
    remove_dots(P).

clean_path(Path) -> % оставляет путь без имени файла в урле
    string:sub_string(Path, 1, string:rchr(Path, $/)).

remove_dots(Path) ->
    P = re:replace(Path, "//+", "/", [global,{return,list}]),
    remove_double_dots(re:replace(P, "/\\.(?=/)", "", [global,{return,list}])).

remove_double_dots(Path) ->
    NewPath = re:replace(Path,"[^/.]+/\\.\\./","",[{return,list}]),
    case string:equal(Path,NewPath) of
        true ->
            Path;
        false ->
            remove_double_dots(NewPath)
    end.

normalize_url(Url) ->
    case re:run(Url,"^https?:",[caseless]) of
        {match, _} ->                   
            compose_url(http_uri:parse(Url));
        nomatch ->
            Url
    end.

scheme_to_string(Scheme) ->
    lists:flatten(io_lib:format("~p",[Scheme])).

compose_url({ok,Result}) ->
    compose_url(Result);
 compose_url({Scheme, UserInfo, Host, Port, Path, Query}) ->
     scheme_to_string(Scheme) ++ "://"
         ++ if_not_empty(UserInfo, UserInfo ++ "@")
         ++ string:to_lower(Host)
         ++ if_not_empty(filter_default_port({Scheme, Port}), ":" ++ integer_to_list(Port))
         ++ Path
         ++ Query.

if_not_empty(Test, Result) -> % не хватает в эрланге тренарного оператора ? : 
    case Test of
        [] ->
            [];
        _ ->
            Result
    end.

filter_default_port({Scheme, Port}) ->
    case {Scheme, Port} of
        {http, 80} ->
            [];
        {https, 443} ->
            [];
        {ftp, 21} ->
            [];
        _ ->
            Port
    end.

skip_query(Url) ->
    QPos = string:str(Url, "?"),
    case QPos of
        0 ->
            FPos = string:str(Url, "#"),
            case FPos of
                0 ->
                    Url;
                _ ->
                    string:substr(Url, 1, FPos - 1)
            end;
        _ ->
            string:substr(Url, 1, QPos - 1)
     end.
