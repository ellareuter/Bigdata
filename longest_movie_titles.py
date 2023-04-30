from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class longest_movie_titles(MRJob):

	MIN_COUNT = 10
	SHOW_LIMIT = 10


	def movie_title(self, movie_id):
		'''
		Convert from movie id to movie title
		'''
		with open("/root/input/u.item", "r", encoding = "ISO-8859-1") as infile:
			reader = csv.reader(infile, delimiter='|')
			next(reader)
			for line in reader:
				if int(movie_id) == int(line[0]):
					return line[1]

	def steps(self):
		'''
		Pipeline of MapReduce tasks
		'''
		return [
			MRStep(mapper=self.mapper1, reducer=self.reducer1),
			MRStep(mapper=self.mapper2, reducer=self.reducer2)
		]

	def mapper1(self, _, line):
		(user_id, movie_id, rating, timestamp) = line.split('\t')
		yield movie_id, rating

	def reducer1(self, movie_id, ratings):
		count = 0
		for _ in ratings:
			count += 1
		if count >= self.MIN_COUNT:
			yield movie_id, count

	def mapper2(self, movie_id, count):
		yield None, (len(self.movie_title(int(movie_id))), movie_id)
  
	def reducer2(self, _, values):
		i = 0
		for title_length, movie_id in sorted(values, reverse=True):
			i += 1
			if i <= self.SHOW_LIMIT:
				yield movie_id, self.movie_title(int(movie_id))

if __name__ == '__main__':
	longest_movie_titles.run()
