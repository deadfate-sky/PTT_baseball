## ----ptt baseball-----------------------------------------------------------------------------
library(dplyr)
library(httr)
library(rvest)
library(lubridate)
library(stringr)
library(rlang)

page <- GET("https://www.ptt.cc/bbs/Baseball/index.html") 

num <- content(page) %>% 
  html_nodes("a.btn.wide") %>% 
  html_attr("href") %>% 
  `[`(2) %>% 
  str_extract_all(., pattern = "\\d") %>% 
  `[[`(1) %>% 
  paste0(collapse = "") %>% 
  as.integer()

iter <- (num+1):(num-100)
DF_link <- data.frame()

## ----GET link-----------------------------------------------------
cat("get link go \n")
for (i in iter){
  #i = 10022
  url <- paste0("https://www.ptt.cc/bbs/Baseball/index",i,".html")
  page <- httr::GET(url)
  
  # title <- content(page) %>%
  #   html_nodes("div.title a") %>%
  #   xml_text()
  
  link <- content(page) %>% 
    html_nodes("div.title a") %>% 
    html_attr("href") %>% 
    paste0("https://www.ptt.cc", .)
  
  author <- content(page) %>% 
    html_nodes("div.author") %>% 
    html_text()
  
  date <- content(page) %>% 
    html_nodes("div.date") %>% 
    html_text()
  
  if (sum(author=="-")) date <- date[!author=="-"]
  if (sum(author=="-")) author <- author[!author=="-"]  
  
  idx <- as.Date(paste0("2020/",date)) == Sys.Date()
  if (sum(idx)==0) break
  
  DF_link <- rbind(DF_link, data.frame(link = link,
                             author = author, 
                             date = date)[idx,])
  cat("link:", url, "  len:", length(link), "\n")
  Sys.sleep(1.0374921)
}


## ----scrap article-------------------------------------------------
DF_article <- data.frame()
cat("article go \n")
for (i in seq_along(DF_link$link)){
  # i = 7
  page <- httr::GET(DF_link$link[i])
  
  metaline <- content(page) %>% 
    html_nodes("span.article-meta-value") %>% 
    html_text()
  
  #author
  author <- metaline[1]
  #title
  title <- metaline[3]
  #date
  date_time <- metaline[4] %>% parse_date_time("md-HMS-y")
  
  text <- content(page) %>% 
    html_nodes("div#main-content.bbs-screen.bbs-content") %>% 
    html_text() %>% 
    str_split(pattern = "--") %>% 
    `[[`(1) %>% 
    `[`(1) %>% 
    str_split_fixed(pattern = "\\n", n = Inf) %>% 
    `[`(-1) %>% 
    str_flatten()
  
  ip <- content(page) %>% 
    html_nodes("span.f2") %>% 
    html_text() %>% 
    str_subset(pattern = "(發信站:|來自)") %>% 
    str_split(pattern = " ", simplify = TRUE) %>% 
    str_subset(pattern = "^\\d")
  
  country <- content(page) %>% 
    html_nodes("span.f2") %>% 
    html_text() %>% 
    str_subset(pattern = "(發信站:|來自)") %>%  
    str_split(pattern = " ", simplify = TRUE) %>% 
    `[`(6) %>% 
    str_remove_all(pattern = "[()\n]")
  if (is_empty(ip)) ip <- NA
  if (is_empty(country)) country <- NA
  
  DF_article <- rbind(DF_article,
        data.frame(
          link = DF_link$link[i], 
          author = author, 
          title = title, 
          date_time = date_time,
          text = text,
          ip = ip,
          country = country
          )
        )
  cat("article done:", DF_link$link[i], i, "\n")
  Sys.sleep(1+abs(rnorm(1)))
}


## ----push content-------------------------------------------------
DF_push <- data.frame()
cat("push content go \n")
for (i in seq_along(DF_link$link)){
  #i = 23
  page <- httr::GET(DF_link$link[i])

  push_type <- content(page) %>% 
    html_nodes("span.hl.push-tag") %>% 
    html_text()
  if (rlang::is_empty(push_type)) push_type <- NA
  
  push_id <- content(page) %>% 
    html_nodes("span.f3.hl.push-userid") %>% 
    html_text()
  if (rlang::is_empty(push_id)) push_id <- NA
  
  push_content<- content(page) %>% 
    html_nodes("span.f3.push-content") %>% 
    html_text() %>% 
    str_remove_all(pattern = ": ")
  if (rlang::is_empty(push_content)) push_content <- NA
  
  push_date <- content(page) %>% 
    html_nodes("span.push-ipdatetime") %>% 
    html_text() %>% 
    str_remove(pattern = " ") %>% 
    str_remove_all("\\n") %>% 
    paste0(year(DF_article$date_time[i])," ", .) %>% 
    ymd_hm()
  if (rlang::is_empty(push_date)) push_date <- NA
  
  DF_push <- rbind(DF_push,
        data.frame(
          link = DF_link$link[i],
          push_type = push_type,
          push_id = push_id,
          push_content = push_content,
          push_date = push_date
          )
        )
  cat("push done:", DF_link$link[i], "  push_num:" , length(push_content), "\n")
  Sys.sleep(1+abs(rnorm(1)))
}


 
which(
  "https://www.ptt.cc/bbs/Baseball/M.1605754567.A.84F.html"==DF_link$link
)

log <- paste0(Sys.time(), "  link:", nrow(DF_link), "  ", "article:",nrow(DF_article))
cat(log, "\n")
log_push <- count(DF_push, link)



write(log, 
      file = "log_df.txt",
      append = file.exists("log_df.txt")
      )
cat(log_push$n, "\n", file = "log_df.txt", fill = TRUE, append = file.exists("log_df.txt"))

## ----save database-----------------------------------
library(DBI)
library(RSQLite)
con <- dbConnect(RSQLite::SQLite(), dbname = "PTTbaseball.db")

dbWriteTable(con, 
             "link_dataframe", 
             DF_link,
             append = dbExistsTable(con, "link_dataframe"))

dbWriteTable(con, 
             "article", 
             DF_article,
             append = dbExistsTable(con, "article"))

dbWriteTable(con, 
             "push", 
             DF_push,
             append = dbExistsTable(con, "push"))

dbListTables(con)

log_db <- sapply(dbListTables(con),
       function(x) nrow(dbReadTable(con,x))
       )  




t(data.frame(log_db)) %>% 
  as_tibble %>% 
  mutate(time = Sys.time()) %>% 
  readr::write_csv(path = "log_db.csv",
                   append = file.exists("log_db.csv"))






