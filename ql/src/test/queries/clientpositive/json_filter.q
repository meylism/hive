DROP TABLE IF EXISTS tweets;

set hive.io.filter.sparser = true;

create table tweets (id string, text string, attachments string, author_id string, context_annotations string,
conversation_id string, created_at string, entities string, geo string, in_reply_to_user_id string, lang string,
non_public_metrics string, organic_metrics string, possibly_sensitive string, promoted_metrics string,
public_metrics string, referenced_tweets string, reply_settings string, source string, withheld string, matching_rules string
) STORED AS JSONFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/twitter_large.json" INTO TABLE tweets;

select text from tweets where text = "elon";