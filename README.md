# erlang_web_crawler

продолжение истории <https://github.com/onokhov/crawler/blob/master/README.md>

example of usage:
```
Eshell V8.1  (abort with ^G)
1> с(crawler).
{ok,crawler}
2> crawler:crawler("https://glav.su/forum/", 10).
```


# постановка задачи


Реализовать web-crawler, рекурсивно скачивающий сайт (идущий по ссылкам вглубь). Crawler должен скачать документ по указанному URL и продолжить закачку по ссылкам, находящимся в документе.
 - Crawler должен поддерживать дозакачку.
 - Crawler должен грузить только текстовые документы -   html, css, js (игнорировать картинки, видео, и пр.)
 - Crawler должен грузить документы только одного домена (игнорировать сторонние ссылки)
 - Crawler должен быть многопоточным (какие именно части параллелить - полностью ваше решение)

