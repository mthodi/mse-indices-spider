# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from scrapy.exceptions import CloseSpider
from datetime import datetime
import pytz
import os, sys

class MseIndicesSpider(scrapy.Spider):
    name = 'mse_indices'
    allowed_domains = ['mse.co.mw']
    start_urls = ['https://mse.co.mw/']

    def parse(self, response):
        # TODO: verify that it is a daily report
        csv_url =  'https://mse.co.mw/index.php?route=module/graph/download'
        today_in_malawi = datetime.now(pytz.timezone('Africa/Blantyre'))
        if today_in_malawi.isoweekday() not in range (1,6): # weekday?
            with open(os.path.join(sys.path[0], 'status.txt'), 'w') as file:
                file.write('CLOSED')
            raise CloseSpider('Not a weekday.')
        yield Request(
            url=csv_url,
            callback=self.save_csv
        )
    
    def save_csv(self, response):
        """Saves the indices prices as CSV"""
        output_file= datetime.today().strftime('indices_close_%d_%B_%Y.csv')
        self.logger.info("[+] Saving report at {}".format(output_file))
        with open(os.path.join(sys.path[0], output_file), 'wb') as file:
            file.write(response.body)
        with open(os.path.join(sys.path[0], 'status.txt'), 'w') as file:
            file.write(output_file)



