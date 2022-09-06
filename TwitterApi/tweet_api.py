from collections import defaultdict
from logging import getLogger, basicConfig, DEBUG
import tweepy
import json
import pandas as pd
from collections import defaultdict
from time import time, sleep
from os import mkdir
from tqdm import tqdm


def convert2str(x):
    try:
        return str(x)
    except:
        return ''


def fix_column(df: pd.DataFrame):
    for c in df.columns:
        if 'id' in c:
            df[c] = df[c].apply(convert2str)
    return df


def flatten_referenced_tweets(t):
    if 'referenced_tweets' not in t:
        return t
    for refrence in t.pop('referenced_tweets'):
        t[refrence.type + '_id'] = refrence.id
    return t


def flatten_dict(x):
    res = defaultdict(lambda: None)
    for k, v in x.items():
        if type(v) == dict:
            for kk, vv in flatten_dict(v).items():
                res[f'{k}_{kk}'] = vv
        else:
            res[k] = v
    return res


def create_dataframe_from_dictlist(dictlist, pre=''):
    features = set()
    for i in dictlist:
        features = features.union(i.keys())
    features = list(features)
    features.sort()
    return pd.DataFrame({
        pre + k: [i[k] for i in dictlist] for k in features
    })


def create_new_directory(path, logger):
    try:
        mkdir(path)
    except Exception as e:
        logger.warning(e, exc_info=1)


class TweetCrawlerAcademic:
    """:class:`TweetCrawlerAcademic`

    Parameters
    ----------
    config_file
        :class:`str` path of config file as string. 
            Config must have the `bearer_token` key stored as JSON formatted file.
    log_path
        :class:`str` path of log file. Default value is `'Crawler.log'`.
    """


    def __init__(self, config_file, log_path='Crawler.log'):
        self.bearer_token = None
        self.client = None
        self.readConfig(config_file)
        self.boot()
        basicConfig(filename=log_path, filemode='a', format='%(levelname)s:%(name)s: [%(asctime)s] %(message)s')


    def readConfig(self, config_file):
        accounts = json.load(open(config_file))
        self.bearer_token = accounts['bearer_token']


    def boot(self):
        self.client = tweepy.Client(bearer_token=self.bearer_token)


    def get_recent_tweets(self, query, fields = ['context_annotations', 'created_at']):

        tweets = self.client.search_recent_tweets(query=query, tweet_fields=fields, max_results=100)
        for tweet in tweets.data:
            print(tweet.text)
            if len(tweet.context_annotations) > 0:
                print(tweet.context_annotations)

    
    def __create_params__(self, prm_type, **params):
        
        default_params = {
            'tweet_fields': ['text', 'id', 'lang', 'author_id', 'created_at', 'public_metrics', 'referenced_tweets'],
            'user_fields': ['id', 'name', 'username', 'created_at', 'public_metrics'],
            'expansions': ['author_id', 'referenced_tweets.id']
        }

        if prm_type == 'default':
            params = default_params
        elif prm_type == 'append':
            prms = defaultdict(list)
            for k, v in params.items():
                prms[k] = v
            for k, v in default_params.items():
                prms[k] += v
            params = prms
        
        if 'max_results' not in params.keys():
            params['max_results'] = 500
        
        return params
    

    def __get_respond_dfs__(self, respond):
        tweet_respond = [flatten_referenced_tweets(flatten_dict(i)) for i in respond.data]

        user_respond, refrence_respond = [], []

        if 'users' in respond.includes:
            user_respond = [flatten_dict(i) for i in respond.includes['users']]
        if 'tweets' in respond.includes:
            refrence_respond = [flatten_dict(i) for i in respond.includes['tweets']]
        for twt in refrence_respond:
            if 'referenced_tweets' in twt:
                twt.pop('referenced_tweets')
        return  {
            'tweets': create_dataframe_from_dictlist(tweet_respond),
            'users': create_dataframe_from_dictlist(user_respond),
            'refrences': create_dataframe_from_dictlist(refrence_respond)
        }


    def __save_df_dict__(self, df_dict, path):
        for name, df in df_dict.items():
            fix_column(df).to_csv(f'{path}/{name}.csv', index=False)
    

    def __get_date_range__(self, df_dict):
        df = df_dict['tweets']
        return df['created_at'].min(), df['created_at'].max()


    def get_all_tweets(
        self, 
        query, 
        start_time, 
        end_time,
        store_path,
        field_type = 'default',
        sleep_time = 5,
        **params
    ):
        """
        Parameters
        ----------
        query : str
            One query for matching Tweets. Up to 1024 characters.
        start_time : datetime.datetime | str | None
            YYYY-MM-DDTHH:mm:ssZ (ISO 8601/RFC 3339). The oldest UTC timestamp
            from which the Tweets will be provided. Timestamp is in second
            granularity and is inclusive (for example, 12:00:01 includes the
            first second of the minute).
        end_time : datetime.datetime | str | None
            YYYY-MM-DDTHH:mm:ssZ (ISO 8601/RFC 3339). Used with ``start_time``.
            The newest, most recent UTC timestamp to which the Tweets will be
            provided. Timestamp is in second granularity and is exclusive (for
            example, 12:00:01 excludes the first second of the minute).
        store_path: str
            path in which crawled data will stored.
        field_type: str
            Costumize which features of tweet to return.
                `'default'`: return default features.
                `'append'`: return default features + feature inside `**params`
                other: return only features inside `**params`
        sleep_time: int
            How many seconds to wait befor requstring for new pages of data.

        Returns
        -------
        None | Crawled data will store in `store_path` as `.csv` files.
        """
        logger = getLogger(name=query)
        logger.setLevel(DEBUG)
        logger.info('Start Crawling...')

        params = self.__create_params__(field_type, **params)
        create_new_directory(store_path, logger)

        #logger.debug(params)
        #logger.debug(query)
        #logger.debug(f'{start_time}\t{end_time}')

        total_count = 0

        for i, respond in tqdm(enumerate(tweepy.Paginator( 
                            self.client.search_all_tweets,
                            query = query,
                            start_time=start_time,
                            end_time = end_time,
                            **params))):
            
            initail_time = time()

            if respond.data is None:
                continue

            if len(respond.data) == 0:
                continue

            str_path = f'{store_path}/page{i+1}'
            create_new_directory(str_path, logger)

            df_dict = self.__get_respond_dfs__(respond)
            self.__save_df_dict__(df_dict, str_path)

            total_count += respond.meta['result_count']
            l, u  = self.__get_date_range__(df_dict)

            logger.info(
                f"""count: {respond.meta["result_count"]} \t Total: {total_count}"""
                # from {respond.meta["oldest_id"]} to {respond.meta["newest_id"]} collected.
                # from {l} to {u}
                # """
            )

            delta_t = time() - initail_time
            if delta_t < sleep_time:
                sleep(sleep_time - delta_t)
        
        logger.info('Crawling Finished...')
