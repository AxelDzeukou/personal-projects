# %%
#using scrapy
import scrapy
import json
import os
from datetime import datetime

SCRAPED_FIGHTS_FILE = 'scraped_fights.json'
SCRAPED_EVENTS_FILE = 'scraped_events.json'

# Keep track of previously scraped fights and events
if os.path.exists(SCRAPED_FIGHTS_FILE):
    with open(SCRAPED_FIGHTS_FILE, 'r') as f:
        scraped_fights_set = set(json.load(f))
else:
    scraped_fights_set = set()

if os.path.exists(SCRAPED_EVENTS_FILE):
    with open(SCRAPED_EVENTS_FILE, 'r') as f:
        scraped_events_set = set(json.load(f))
else:
    scraped_events_set = set()


class UFCFightSpider(scrapy.Spider):
    name = "ufc_fights"
    allowed_domains = ["ufcstats.com", "https://www.ufc.com/athletes/all"]
    start_urls = ["http://ufcstats.com/statistics/events/completed?page=all"]

    number_of_events = 20  # limit number of events to scrape

    fight_fields = [
        'fighter_A','fighter_B',
        'fighter_A_KD', 'fighter_B_KD',
        'fighter_A_SIG_STR', 'fighter_B_SIG_STR',
        'fighter_A_SIG_STR%', 'fighter_B_SIG_STR%',
        'fighter_A_TOTAL_STR', 'fighter_B_TOTAL_STR',
        'fighter_A_TD', 'fighter_B_TD',
        'fighter_A_TD%', 'fighter_B_TD%',
        'fighter_A_SUB_ATT', 'fighter_B_SUB_ATT',
        'fighter_A_REV', 'fighter_B_REV',
        'fighter_A_CTRL', 'fighter_B_CTRL',
        'Winner',
        'linktofight'
    ]

    def __init__(self, *args, **kwargs):
        super(UFCFightSpider, self).__init__(*args, **kwargs)
        self.fight_data = {key: [] for key in self.fight_fields}
        self.events_to_scrape = []  # store tuples of (event_url, event_date)

    def check_html(self, link):
        return link.endswith('.html')

    def parse(self, response):
        # Get all event links
        events_completed = response.css('tr.b-statistics__table-row td i a::attr(href)').getall()

        for link in events_completed:
            if link not in scraped_events_set:
                # Parse event date from the page
                date_text = response.css(f'a[href="{link}"] ::attr(title)').get()
                if date_text:
                    try:
                        event_date = datetime.strptime(date_text, "%b %d, %Y")
                    except:
                        event_date = datetime.today()
                else:
                    event_date = datetime.today()
                self.events_to_scrape.append((link, event_date))

        # Sort events chronologically
        self.events_to_scrape.sort(key=lambda x: x[1])

        self.logger.info(f"Found {len(self.events_to_scrape)} new events to scrape (chronological).")

        # Scrape events in order
        for event_url, _ in self.events_to_scrape:
            if self.check_html(event_url+'.html') and 'http://ufcstats.com/event-details/' in event_url:
                yield response.follow(event_url, callback=self.parse_event_ordered)

    def parse_event_ordered(self, response):
        fight_links = response.css('tr[data-link]::attr(data-link)').getall()
        fight_links = [link for link in fight_links if link not in scraped_fights_set]

        # Optional: Get event date to store with each fight
        date_text = response.css('li.b-list__box-list-item span.b-list__box-list-item-content::text').get()
        try:
            event_date = datetime.strptime(date_text.strip(), "%b %d, %Y") if date_text else datetime.today()
        except:
            event_date = datetime.today()

        # Store fight links with date for ordering
        self.fights_to_scrape = [(link, event_date) for link in fight_links]

        # Sort by event date (all fights in this event have same date)
        self.fights_to_scrape.sort(key=lambda x: x[1])

        for fight_url, _ in self.fights_to_scrape:
            if self.check_html(fight_url+'.html') and "fight-details" in fight_url:
                yield response.follow(fight_url, callback=self.parse_fight_ordered, meta={'fight_url': fight_url})

    def parse_fight_ordered(self, response):
        fight_url = response.meta['fight_url']
        self.fight_data['linktofight'].append(fight_url)
        scraped_fights_set.add(fight_url)

        # Extract winner
        winner = None
        winner_i = response.css('div.b-fight-details__person i.b-fight-details__person-status.b-fight-details__person-status_style_green')
        if winner_i:
            winner_container = winner_i.xpath('ancestor::div[contains(@class,"b-fight-details__person")]')[0]
            winner_tag = winner_container.css('div h3 a::text').get()
            winner = winner_tag.strip() if winner_tag else None
        else:
            gray_status = response.css('div.b-fight-details__person i.b-fight-details__person-status.b-fight-details__person-status_style_gray::text').get()
            winner = gray_status.strip() if gray_status and gray_status.strip() in ['NC', 'D'] else "Unknown"

        self.fight_data['Winner'].append(winner)

        # Extract stats table
        fight_stats_tr = response.css('tr')[1]
        if not fight_stats_tr:
            for key in self.fight_fields:
                if key != 'Winner':
                    self.fight_data[key].append(None)
            return

        tds = fight_stats_tr.css('td')
        try:
            self.fight_data['fighter_A'].append(tds[0].css('p a::text').get(default='').strip())
            self.fight_data['fighter_B'].append(tds[0].css('p')[1].css('a::text').get(default='').strip())
            self.fight_data['fighter_A_KD'].append(tds[1].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_KD'].append(tds[1].css('p')[1].css('::text').get(default='').strip())
            self.fight_data['fighter_A_SIG_STR'].append(tds[2].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_SIG_STR'].append(tds[2].css('p')[1].css('::text').get(default='').strip())
            self.fight_data['fighter_A_SIG_STR%'].append(tds[3].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_SIG_STR%'].append(tds[3].css('p')[1].css('::text').get(default='').strip())
            self.fight_data['fighter_A_TOTAL_STR'].append(tds[4].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_TOTAL_STR'].append(tds[4].css('p')[1].css('::text').get(default='').strip())
            self.fight_data['fighter_A_TD'].append(tds[5].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_TD'].append(tds[5].css('p')[1].css('::text').get(default='').strip())
            self.fight_data['fighter_A_TD%'].append(tds[6].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_TD%'].append(tds[6].css('p')[1].css('::text').get(default='').strip())
            self.fight_data['fighter_A_SUB_ATT'].append(tds[7].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_SUB_ATT'].append(tds[7].css('p')[1].css('::text').get(default='').strip())
            self.fight_data['fighter_A_REV'].append(tds[8].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_REV'].append(tds[8].css('p')[1].css('::text').get(default='').strip())
            self.fight_data['fighter_A_CTRL'].append(tds[9].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_CTRL'].append(tds[9].css('p')[1].css('::text').get(default='').strip())
        except Exception:
            for key in self.fight_fields:
                if key != 'Winner' and len(self.fight_data[key]) < len(self.fight_data['Winner']):
                    self.fight_data[key].append(None)

    def closed(self, reason):
        import pandas as pd
        df = pd.DataFrame(self.fight_data)
        df.to_excel("ufc_fights.xlsx", index=False)
        self.logger.info("Saved fights data to ufc_fights.xlsx")

        # Save scraped fights and events
        with open(SCRAPED_FIGHTS_FILE, 'w') as f:
            json.dump(list(scraped_fights_set), f)
        with open(SCRAPED_EVENTS_FILE, 'w') as f:
            json.dump(list(scraped_events_set.union({link for link, _ in self.events_to_scrape})), f)



# #using scrapy
# import scrapy

# # %%
# scraped_fights={}

# class UFCFightSpider(scrapy.Spider):
#     name = "ufc_fights"
#     allowed_domains = ["ufcstats.com", "https://www.ufc.com/athletes/all"]  # Add more domains here
#     start_urls = ["http://ufcstats.com/statistics/events/completed?page=all"]

#     number_of_events = 20  # limit number of events to scrape

#     # fight details fields
#     fight_fields = [
#         'fighter_A','fighter_B',
#         'fighter_A_KD', 'fighter_B_KD',
#         'fighter_A_SIG_STR', 'fighter_B_SIG_STR',
#         'fighter_A_SIG_STR%', 'fighter_B_SIG_STR%',
#         'fighter_A_TOTAL_STR', 'fighter_B_TOTAL_STR',
#         'fighter_A_TD', 'fighter_B_TD',
#         'fighter_A_TD%', 'fighter_B_TD%',
#         'fighter_A_SUB_ATT', 'fighter_B_SUB_ATT',
#         'fighter_A_REV', 'fighter_B_REV',
#         'fighter_A_CTRL', 'fighter_B_CTRL',
#         'Winner',
#         'linktofight'
#     ]

#     def __init__(self, *args, **kwargs):
#         super(UFCFightSpider, self).__init__(*args, **kwargs)
#         self.fight_data = {key: [] for key in self.fight_fields}

#         # whenever the code runs i want it to work on the new events and the scraped new fights added to the existing fights        

#     def check_html(self, link):
#         return link.endswith('.html')
    

#     def parse(self, response):
#         # Select x event links
#         # events_completed = response.css('tr.b-statistics__table-row td i a::attr(href)').getall()[:self.number_of_events]

    
#         # # Select event links
#         events_completed = response.css('tr.b-statistics__table-row td i a::attr(href)').getall()

#         unique_event_links = []
#         seen = set()
#         for link in events_completed:
#             if link not in seen:
#                 unique_event_links.append(link)
#                 seen.add(link)

#         self.logger.info(f"Found {len(unique_event_links)} events, scraping each.")

#         #for each event link check if its a real link then proceed to parse_event to extract details of all fights
#         for event_url in unique_event_links:
#             # print(f"Processing event URL: {event_url}")
#             if self.check_html(event_url+'.html') and 'http://ufcstats.com/event-details/' in event_url:
#                 yield response.follow(event_url, callback=self.parse_event)
        
        

#     def parse_event(self, response):
#         # Collect unique fight detail links from event page
#         fight_links = response.css('tr[data-link]::attr(data-link)').getall()

#         unique_fight_links = []
#         seen = set()
#         for link in fight_links:
#             if link not in seen:
#                 unique_fight_links.append(link)
#                 seen.add(link)
        
#         self.logger.info(f"Found {len(unique_fight_links)} fights in event {response.url}")

#         #for each link if its a legit fight link then proceed to parse_fight to extract fight details
#         for fight_url in unique_fight_links:
#             # print(f"Processing fight URL: {fight_url}")
#             # if self.check_html(fight_url+'.html') and "fight-details" in fight_url and fight_url=="http://ufcstats.com/fight-details/33e301872e2b5680":
#             if self.check_html(fight_url+'.html') and "fight-details" in fight_url:
#                 yield response.follow(fight_url, callback=self.parse_fight)

#     def parse_fight(self, response):
#         # Store the fight link
#         self.fight_data['linktofight'].append(response.url)
#         # Extract winner
#         winner = None
#         winner_i = response.css('div.b-fight-details__person i.b-fight-details__person-status.b-fight-details__person-status_style_green')
#         if winner_i:
#             winner_container = winner_i.xpath('ancestor::div[contains(@class,"b-fight-details__person")]')[0]
#             winner_tag = winner_container.css('div h3 a::text').get()
#             winner = winner_tag.strip() if winner_tag else None
#         else:
#             # Check for NC or D results
#             gray_status = response.css('div.b-fight-details__person i.b-fight-details__person-status.b-fight-details__person-status_style_gray::text').get().strip()
#             if gray_status in ['NC', 'D']:
#                 winner = gray_status
#             else:
#                 self.logger.error(f"Error parsing winner for fight {response.url}")
#                 winner = "Unknown"

#         self.fight_data['Winner'].append(winner)

#         # Extract fight stats table - the second 'tr' element (index 1)
#         fight_stats_tr = response.css('tr')[1]
#         if not fight_stats_tr:
#             self.logger.warning(f"No fight stats table found for fight {response.url}")
#             # Append Nones for all fields except winner to keep lengths consistent
#             for key in self.fight_fields:
#                 if key != 'Winner':
#                     self.fight_data[key].append(None)
#             return

#         tds = fight_stats_tr.css('td')

#         try:
#             self.fight_data['fighter_A'].append(tds[0].css('p a::text').get(default='').strip())
#             self.fight_data['fighter_B'].append(tds[0].css('p')[1].css('a::text').get(default='').strip())

#             self.fight_data['fighter_A_KD'].append(tds[1].css('p::text').get(default='').strip())
#             self.fight_data['fighter_B_KD'].append(tds[1].css('p')[1].css('::text').get(default='').strip())

#             self.fight_data['fighter_A_SIG_STR'].append(tds[2].css('p::text').get(default='').strip())
#             self.fight_data['fighter_B_SIG_STR'].append(tds[2].css('p')[1].css('::text').get(default='').strip())

#             self.fight_data['fighter_A_SIG_STR%'].append(tds[3].css('p::text').get(default='').strip())
#             self.fight_data['fighter_B_SIG_STR%'].append(tds[3].css('p')[1].css('::text').get(default='').strip())

#             self.fight_data['fighter_A_TOTAL_STR'].append(tds[4].css('p::text').get(default='').strip())
#             self.fight_data['fighter_B_TOTAL_STR'].append(tds[4].css('p')[1].css('::text').get(default='').strip())

#             self.fight_data['fighter_A_TD'].append(tds[5].css('p::text').get(default='').strip())
#             self.fight_data['fighter_B_TD'].append(tds[5].css('p')[1].css('::text').get(default='').strip())

#             self.fight_data['fighter_A_TD%'].append(tds[6].css('p::text').get(default='').strip())
#             self.fight_data['fighter_B_TD%'].append(tds[6].css('p')[1].css('::text').get(default='').strip())

#             self.fight_data['fighter_A_SUB_ATT'].append(tds[7].css('p::text').get(default='').strip())
#             self.fight_data['fighter_B_SUB_ATT'].append(tds[7].css('p')[1].css('::text').get(default='').strip())

#             self.fight_data['fighter_A_REV'].append(tds[8].css('p::text').get(default='').strip())
#             self.fight_data['fighter_B_REV'].append(tds[8].css('p')[1].css('::text').get(default='').strip())

#             self.fight_data['fighter_A_CTRL'].append(tds[9].css('p::text').get(default='').strip())
#             self.fight_data['fighter_B_CTRL'].append(tds[9].css('p')[1].css('::text').get(default='').strip())

# # the first table is there but maybe there is a difference in the format
#         except Exception as e:
#             self.logger.error(f"Error parsing fight stats for {response.url}: {e}")
#             # Append None to all fields except Winner to maintain consistent list lengths
#             for key in self.fight_fields:
#                 if key != 'Winner' and len(self.fight_data[key]) < len(self.fight_data['Winner']):
#                     self.fight_data[key].append(None)

#     def closed(self, reason):
#         # Called when spider finishes scraping - output results to csv
#         import pandas as pd

#         # why does ['http://ufcstats.com/fight-details/33e301872e2b5680'] work by itself but not with all links to fights?
#         #chatgpt says its because you are scraping a lot at the same time. try scraping into chunks or use a different scraper
#         for column in self.fight_data:
#             print(column)
#             print('length '+str(len(self.fight_data[column])))
        

#         # get links where field is empty
#         #parse fight details for those links then export
#         number_of_fights=8279 
#         # df = pd.DataFrame({k: v[:number_of_fights] for k, v in self.fight_data.items()})
#         df = pd.DataFrame(self.fight_data)
#         self.logger.info(f"Scraped {len(df)} fights.")
#         # Save dataframe or do whatever you want
#         df.to_excel("ufc_fights.xlsx", index=False)
#         self.logger.info("Saved fights data to ufc_fights.xlsx")
    
#     def append_new_fights(self, response):


# %%
from scrapy.crawler import CrawlerProcess

import time
start_time = time.time()

process = CrawlerProcess(settings={
    "LOG_LEVEL": "DEBUG",  # or "DEBUG" for even more detail
})
process.crawl(UFCFightSpider)
process.start()
print("Scraping completed.")
print(f"Total execution time: {time.time() - start_time:.2f} seconds")


# %%
