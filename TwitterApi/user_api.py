import tweepy
import json
import pandas as pd
import time

class UserCrawler:
    def __init__(self):
        self.consumer_keys = []
        self.consumer_secrets = []
        self.auth = []
        self.target_user_path = 'data/data_stream_filtered_onlyUserId.csv'
        self.readConfig()
        self.boot()


    def readConfig(self):
        accounts = json.load(open("configs/accounts.json"))
        self.consumer_keys = accounts['consumer_keys']
        self.consumer_secrets = accounts['consumer_secrets']

    def boot(self):
        for key, secret in zip(self.consumer_keys, self.consumer_secrets):
            self.auth.append(tweepy.OAuthHandler(key, secret))
        self.api = [tweepy.API(x) for x in self.auth]

    def read_target_user(self):
        all_user_df=pd.read_csv(self.target_user_path)
        # test_df = all_user_df[:500]
        user_id_list = all_user_df['0'].tolist()
        return user_id_list

    def get_user_by_id(self, api,id):
        user = api.get_user(user_id=id)
        return user

    def get_users_by_ids(self, api, list_id):
        try:
            users = api.lookup_users(user_id=list_id)
            return users
        except tweepy.TweepError as err:
            if err is not None:
                if err.response.status_code == 404:
                    print('user not found')
                elif err.response.status_code == 403:
                    print('suspended user')
                elif err.response.status_code == 401:
                    print(f'Not authorized')
                elif err.response.status_code == 429:
                    print('Rate limit exceeded, waiting...')
                    time.sleep(5 * 60)
                else:
                    print(err)

    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def main(self):

        user_id_list = self.read_target_user()

        user_data_list = []
        counter_chunk = 0
        for chunk_id in self.chunks(user_id_list, 100):

            users_list = self.get_users_by_ids(self.api[0], chunk_id)
            for eachUser in users_list:
                # print(eachUser._json)
                # print('=============================================================================')
                user_data_list.append(eachUser._json)
            counter_chunk += 1
            print('counter chunk is : ' + str(counter_chunk))

        with open("stream_filtered_users.json", encoding='utf8', mode="w") as final:
            json.dump(user_data_list, final, indent=4)



