import tweepy
import random


class TwitterStreamHandler(tweepy.StreamListener):
	def __init__(self):
		super().__init__()
		self.sequence = 0
		self.tweets_data = list()
		self.tag_counter = {}

	def on_status(self, status):
		tags=[]
		for tweet in status.entities.get('hashtags'):
			tags.append(tweet['text'])
		if len(tags) != 0:
			self.sequence += 1
			print("The number of tweets with tags from the beginning: " + str(self.sequence))
			if self.sequence <= 150:
				self.tweets_data.append(status)
				self.increment_tag_counter(tags)
			else:
				self.fixed_size_sampling(status, tags)

	def fixed_size_sampling(self, status, tags):
		tweet_seq = random.randint(0, self.sequence)
		if tweet_seq < 150:
			replace_tweet = self.tweets_data[tweet_seq]
			self.tweets_data[tweet_seq] = status
			self.decrement_tag_counter(replace_tweet)
			self.increment_tag_counter(tags)

	def increment_tag_counter(self, tags):
		for tag in tags:
			self.tag_counter[tag] = self.tag_counter.get(tag, 0) + 1
		frequent_tags = sorted(sorted(list(self.tag_counter.items())), key=lambda x: x[1], reverse=True)
		count=0
		prev=-1
		for tag in frequent_tags:
			if tag[1] != prev:
				count += 1
				prev = tag[1]
			print(tag[0], ":", tag[1])
			if count == 5:
				break
		print("\n", end="")

	def decrement_tag_counter(self, status):
		tags = []
		for tweet in status.entities.get('hashtags'):
			tags.append(tweet['text'])
		for tag in tags:
			if self.tag_counter[tag] > 1:
				self.tag_counter[tag] -= 1
			else:
				del(self.tag_counter[tag])


if __name__ == "__main__":
	api_key = tweepy.OAuthHandler("ClUcrlPdQSfJ2WQ8O2AYubcYO", "87Qzrh3lrukaOjCEaafG7CznzfZ8ggnQeTemNQMCZPu333Q5rQ")
	api_key.set_access_token("1200989670923833344-zCMdKlph30iFBPQB8PAkhg3LGeoqSe", "3ZTTJX31bv0YUkU9BAhMrr1Agji0WmYyzi79OZjdZQDBU")
	tweepy_api = tweepy.API(api_key)
	reservoir_sampling_handler = TwitterStreamHandler()
	twitter_stream = tweepy.Stream(auth=tweepy_api.auth, listener=reservoir_sampling_handler)
	twitter_stream.filter(track=['#'])
	# twitter_stream.sample()
