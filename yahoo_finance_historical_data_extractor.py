#!/usr/bin/env python

# -------------------------------------------------------------------------- #
# Developer: Andrew Kirfman                                                  #
# Project: Financial Application                                             #
#                                                                            #
# File: ./investment_aggregator_emailer.py                                   #
# -------------------------------------------------------------------------- #

# -------------------------------------------------------------------------- #
# System Includes                                                            #
# -------------------------------------------------------------------------- #

import os
import re
import sys
import time
import datetime
import copy
import shutil
import json

# -------------------------------------------------------------------------- #
# Custom Includes                                                            #
# -------------------------------------------------------------------------- #

sys.path.append("./HTTP_Request_Randomizer")

from http.requests.proxy.requestProxy import RequestProxy
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool

STOCK_FILE = "./stock_list.txt"
FILTERED_STOCK_FILE = "./filtered_stock_list.txt"
SPECIAL_CHAR_LIST = ['+', '*', '-', '^', '_', '#']
NUM_THREADS = 10

HISTORICAL_DIRECTORY = "./historical_data"
DIVIDEND_DIRECTORY = "./dividend_data"


class BadTickerFile(Exception):
	pass


class CannotCreateDirectory(Exception):
	pass


class YFHistoricalDataExtract(object):
	"""
	Function for grabbing historical stock data from yahoo finance.  Utilizes
	the HTTP_Request_Randomizer library to make proxied function calls so as to
	avoid IPbans from relevant sources.  
	
	<More Info Here!!!>
	"""
	
	def __init__(self, stock_file, data_storage_dir = "./historical_stock_data", threads=10, clear_existing = True):
		"""
		Initializes the proxy server as well as directories that all of
		the read in historical data will be stored to.  
		
		Note: The directory structure could already exist and the data could already be there.  
		It does not always make sense to delete the old data and start again.  If the clear_existing
		variable is set, clear the existing directories.  The default is to clear the existing 
		directories containing historical data and start over.  
		"""
		
		self.proxy_server = RequestProxy()
		self.output_dir = data_storage_dir
		self.ticker_file = stock_file
		self.thread_limit = threads
		
		# If the user asks for it, clear the existing directory structure
		if clear_existing is True:
			self.clear_directories()
		
		# Check to see if the file containing ticker symbols exists
		if not os.exists(stock_file):
			raise BadTickerFile()
		
		# Try to make the directory structure that the data will be stored in
		self.setup_directories()
		
		try:
			os.makedirs("%s/dividends" % self.output_dir)
		except OSError:
			print "[Error]: Could not create directory structure."
			raise CannotCreateDirectory()
			
	def clear_directories(self):
		"""
		Wipe the existing directory structure if it exists.
		"""
		
		os.system("rm -rf %s" % self.output_dir)
			
	def setup_directories(self):
		if not os.exists(self.output_dir):
			try:
				os.makedirs(self.output_dir)
			except OSError as e:
				print "[ERROR]: %s" % str(e)
				raise CannotCreateDirectory()
				
		if not os.exists(self.output_dir + "/dividend_history"):
			try:
				os.makedirs(self.output_dir + "/dividend_history")
			except OsError as e:
				print "[ERROR]: %s" % str(e)
				raise CannotCreateDirectory()
				
	
	
	def get_historical_data(self, threads = 200):
		stock_file = open(self.ticker_file, "r")

		candidates_to_test = []

		pool = ThreadPool(threads)

		for ticker in stock_file.readlines():
			candidates_to_test.append(ticker.strip())

		pool.map(read_ticker_historical, candidates_to_test)
	
    def read_ticker_historical(self, ticker_symbol):
        URL = "https://finance.yahoo.com/quote/%s/history/" % ticker_symbol
        response = None

        # Loop until you get a valid response
        while True:
            try:
                response = self.proxy_server.generate_proxied_request(URL, req_timeout=5)
            except Exception as e:
                print "Exception: %s %s" % (ticker_symbol, str(e))
                return

            if response is None:
                continue

            if response.__dict__['status_code'] == 200:
                break

        response_soup = BeautifulSoup(response.text, 'html5lib')

        # Find all rows in the historical data.
        response_soup = response_soup.find_all("tr")
        response_soup = response_soup[2:]

        json_history_file = open("%s/%s.json" % (self.output_dir, ticker_symbol), "w")
        json_dividend_file = open("%s/%s_dividend.json" % (self.output_dir + "/dividend_history", ticker_symbol), "w")

        historical_data = {
                'Date'      : [],
                'Open'      : [],
                'High'      : [],
                'Low'       : [],
				'Close'     : [],
				'Adj Close' : [],
				'Volume'    : []
				}

		dividend_data = {
				'Date'      : [],
				'Amount'    : []
				}


		for response in response_soup:
			filtered_response = response.find_all("td")

			if len(filtered_response) == 7:

				# Date
				historical_data["Date"].append(filtered_response[0].text)

				# Open
				historical_data["Open"].append(filtered_response[1].text)

				# High
				historical_data["High"].append(filtered_response[2].text)

				# Low
				historical_data["Low"].append(filtered_response[3].text)

				# Close
				historical_data["Close"].append(filtered_response[4].text)

				# Adj Close
				historical_data["Adj Close"].append(filtered_response[5].text)
			elif len(filtered_response) == 2:

				# Date
				dividend_data["Date"].append(filtered_response[0].text)

				# Dividend Amount
				amount = filtered_response[1].text.replace(" Dividend", "")
				dividend_data["Amount"].append(amount)
			else:
				continue

		json_history_file.write(json.dumps(historical_data))
		json_dividend_file.write(json.dumps(dividend_data))

		json_history_file.close()
		json_dividend_file.close()
