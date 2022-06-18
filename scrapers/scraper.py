class Scraper:
    def __init__(self):
        self._scraper = DefaultScraper()

    def set_scraper(self, scraper):
        self._scraper = scraper

    def scrape(self):
        return self._scraper.scrape()


class DefaultScraper:
    def __init__(self):
        pass

    def scrape(self):
        pass
