%%% @author Alexander Onokhov <ccnweb@gmail.com>
%%% @doc http crawler

-module(crawler).

-author("ccnweb@gmail.com").

-export([ crawler/1
        , crawler/2
        , worker/2
        ]).

%% @doc Загружает рекурсивно документы с сайта начиная с Url данного в параметрах.
%%  Параметры: Url начала загрузки и количество потоков.
crawler(Url) ->
    crawler(Url, 1).

%% вызываем рекурсивную процедуру загрузки
crawler(Url, WorkersMaxNum) when WorkersMaxNum >= 1 ->
    {ok,[_,Host,_,_,_]} = parse_url(string:to_lower(Url)),
    inets:start(), % запускаем службы httpc
    ssl:start(),
    erlang:send_after(10000, self(), save_state),    % через 10 секунд хотим сохранить состояние загрузки
    case file:consult(Host ++ ".state") of   % пытаемся восстановить состояние прерванной загрузки
        {ok, [[UrlsRestored, SeenUrlsRestored]]} -> 
            io:format("Resume download session~n"),
            crawler(Host, WorkersMaxNum, UrlsRestored, [], SeenUrlsRestored);
        _ -> 
            crawler(Host, WorkersMaxNum, [Url], [], [])
    end,
    ssl:stop(),  % останавливаем службы httpc
    inets:stop(),
    io:format("Crawler finished~n");
crawler(_,_) ->
    io:format("Usage: crawler:crawler(Url, MaxWorkersNum) Url = string, MaxWorkersNum = integer > 0~n").
    
%% @doc рекурсивная процедура загрузки
%% раздаём задания воркерам,
%% ожидаем сообщения от воркеров, в которых они передают новые задания для загрузки
%% ожидаем сообщение от таймера, по которому сохраняем текущее состояние загрузки в файл
crawler(Host, _SpareWorkersNum, [], [], _SeenUrls) ->  % паттерн завершения работы, всё загружено
    file:delete(Host ++ ".state"); %  удаляем файл состояния загрузки
crawler(Host, SpareWorkersNum, [Url|UrlsLeft], UrlsInProgress, SeenUrls) when SpareWorkersNum > 0 -> % паттерн раздачи заданий по воркерам
    spawn(crawler, worker, [self(), Url]),
    crawler(Host, SpareWorkersNum - 1, UrlsLeft, [Url|UrlsInProgress], SeenUrls); 
crawler(Host, SpareWorkersNum, Urls, UrlsInProgress, SeenUrls) -> % здесь SpareWorkersNum == 0 orelse Urls =:= [], ждём сообщений
    receive
        save_state -> % это от таймера. Сохраним состояние и вернёмся в ожидание
            io:format("saving state...~n"),
            file:write_file(Host ++ ".state", io_lib:format("~p.~n", [[UrlsInProgress ++ Urls, SeenUrls]])),
            erlang:send_after(10000, self(), save_state),    % через 10 секунд снова хотим сохранить состояние загрузки
            crawler(Host, SpareWorkersNum, Urls, UrlsInProgress, SeenUrls); % возвращаемся в ожидание сообщений
        [UrlFetched, NewUrls] -> % воркер прислал новые урлы
            crawler(Host,
                    SpareWorkersNum + 1,
                    %% фильтруем новые задания, не берем те, что уже сделаны или с других хостов
                    [U || U <- NewUrls, is_url_of_host(U, Host) andalso not lists:member(U, SeenUrls) andalso not lists:member(U, UrlsInProgress)], 
                    lists:delete(UrlFetched, UrlsInProgress),
                    [UrlFetched|SeenUrls]
                    )
    after 15000 ->
            io:format("Crawler timed out~n") % что-то пошло не так
    end.

%% @private
worker(ParentPid, Url) -> 
    ParentPid ! [ Url, fetch_and_save(Url) ].

fetch_and_save(Url) ->
    %% асинхронный запрос, чтоб на этапе разбора заголовков можно было отказаться от загрузки нетекстовых документов
    %% проверять через метод HEAD не получается, т.к. некоторые сайты его попросту запрещают
    io:format("fetch ~s~n",[Url]),
    R = httpc:request(get, {Url, []}, [], [{sync, false}, {stream, self}, {full_result, false}]),
    case R of
        {error, Reason} ->
            io:format("Error fetching ~s: ~w~n", [Url, Reason]),
            []; % разбирать ошибки загрузки не хочу, достаточно сообщения в лог. Пусть пользователь сам решает, что делать. 
        {ok, RequestId} ->
            {Html, UrlsToFetch} = parse_html(receive_text_data(RequestId), Url),
            save_to_file(Html, path_to_index(Url)),
            sets:to_list(sets:from_list(UrlsToFetch)) % возвращаем урлы, удалив дубликаты
    end.

receive_error(RequestId, Message) ->
    io:format("receive error: ~s~n",[Message]),
    httpc:cancel_request(RequestId),
    <<>>.

receive_text_data(RequestId) ->
    receive
        {http, {RequestId, stream_start, Headers}} ->
            case [ ok || {"content-type", [$t,$e,$x,$t,$/|_]} <- Headers ] of
                [] -> receive_error(RequestId, "skip non text document");
                _  -> receive_text_data(RequestId, <<>>)
            end;
        {http, {RequestId, {error, Reason}}} -> receive_error(RequestId, lists:flatten(io_lib:format("~p",Reason)))
    after 10000 -> receive_error(RequestId, "receive timeout")
    end.

receive_text_data(RequestId, Acc) ->
    receive
        {http, {RequestId, {error, Reason}}}      -> receive_error(RequestId, lists:flatten(io_lib:format("~p",Reason)));
        {http, {RequestId, stream, BinBodyPart}}  -> receive_text_data( RequestId, <<Acc/binary,BinBodyPart/binary>> );
        {http, {RequestId, stream_end, _Headers}} -> Acc
    after 10000 -> receive_error(RequestId, "receive timeout")
    end.

%% @doc возвращает html с преобразованными ссылками из абсолютных в относительные и список ссылок для загрузки
extract_links([], _Html, ParsedParts, _Pos, _BaseUrl, Links) ->
            {lists:flatten(lists:reverse(ParsedParts)), Links};
extract_links([[{Start, Len}] | Positions], Html, ParsedParts, Pos, BaseUrl, Links) ->
    Link = binary_to_list(binary:part(Html, Start, Len)),
    extract_links( Positions,
                   Html,
                   [path_to_index(skip_query(url_to_relative(Link, BaseUrl))), binary_to_list(binary:part(Html, Start, Pos - Start)) | ParsedParts],
                   Start + Len,
                   BaseUrl,
                   [skip_query(url_to_absolute(Link, BaseUrl)) | Links]
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

save_to_file([],  _Url) -> ok;
save_to_file(Html, Url) ->
    Filename = url_to_filename(Url),
    filelib:ensure_dir(filename:dirname(Filename)++"/"),
    %% io:format("f: ~s, d: ~s~n",[Filename, filename:dirname(Filename)++"/"]),
    file:write_file(Filename, Html).

url_to_filename(Url) ->
    Pos = string:str(Url, "://"),
    "." ++ string:substr(Url, Pos + 2).

path_to_index(Url) -> % если ссылка на каталог, то приписываем к ней index.html
     case lists:last(Url) of
         $/ -> Url ++ "index.html";
         _  -> Url
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
                    string:join(lists:append([["."], [".." || _ <- PreparedB], PreparedP]), "/")
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

is_url_of_host(Url, Host) ->
    case re:run(Url,"^https?://" ++ Host, [caseless]) of
        {match, _} ->                   
            true;
        nomatch ->
            false
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
         ++ if_not_empty(filter_default_port(Scheme, Port), ":" ++ integer_to_list(Port))
         ++ Path
         ++ Query.

if_not_empty([_], Result) -> Result;  % не хватает в эрланге тренарного оператора ?: 
if_not_empty([],       _) -> [].

filter_default_port(http,      80) -> [];
filter_default_port(https,    443) -> [];
filter_default_port(ftp,       21) -> [];
filter_default_port(_Scheme, Port) -> Port.

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
