# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join, Identity


def siteBasedOnSvg(svg):
    if svg.extract().find('"#svg-icon-website"') >= 0:
        return svg.xpath('./ancestor::a[1]/@href').extract_first()


def emailBasedOnSvg(svg):
    if svg.extract().find('"#svg-icon-email"') >= 0:
        return svg.xpath('./ancestor::a[1]//text()').extract_first()


def phoneBasedOnSvg(svg):
    if svg.extract().find('"#svg-icon-earphone"') >= 0:
        return svg.xpath('./ancestor::a[1]/@data-content').extract_first()


def deliveryText(factTag):
    if factTag.xpath('./div[1]//text()'
                     ).extract_first().strip() == 'Liefergebiet':
        return factTag.xpath('./div[2]//text()').extract_first()


def certificatesText(factTag):
    if factTag.xpath('./div[1]//text()'
                     ).extract_first().strip() == 'Zertifikate':
        l = factTag.xpath('./div[2]//text()').extract()
        return ', '.join(filter(lambda x: x not in [' ', ''], l))



def factsText(factTag):
    if factTag.xpath('./div[1]//text()'
                     ).extract_first().strip() == 'Daten und Fakten':
        huj = factTag.xpath('./div[2]')
        factsLoader = FactsItemLoader(item=FactsItem(), selector=huj)
        lis = factsLoader.nested_xpath('.//li')
        lis.add_value('facts', lis.selector)
        fi = factsLoader.load_item()
        return fi['facts']


def angebot(articleTag):
    statusLoader = StatusItemLoader(item=StatusItem(), selector=articleTag)
    iTag = statusLoader.nested_xpath('.//*[@title]')
    aName = iTag.get_xpath('.//ancestor::div[2]//text()',
                           TakeFirst(), str.strip)
    iTag.add_value('status', iTag.selector)
    statItem = statusLoader.load_item()
    statuses = statItem.get('status')
    if statuses:
        statText = ' (' + statuses + ')'
    else:
        statText = ''
    return aName + statText


class WlwItem(scrapy.Item):
    # define the fields for your item here like:
    query = scrapy.Field()
    category = scrapy.Field()
    total_firms = scrapy.Field()
    firmaId = scrapy.Field()
    name = scrapy.Field()
    full_addr = scrapy.Field()
    street = scrapy.Field()
    building = scrapy.Field()
    zip = scrapy.Field()
    city = scrapy.Field()
    phone = scrapy.Field()
    email = scrapy.Field()
    site = scrapy.Field()
    delivery = scrapy.Field()
    facts = scrapy.Field()
    certificates = scrapy.Field()
    angebots = scrapy.Field()
    # debug fields
    page = scrapy.Field()
    totalOnPage = scrapy.Field()
    queryCat = scrapy.Field()


class StatusItem(scrapy.Item):
    status = scrapy.Field()


class FactsItem(scrapy.Item):
    facts = scrapy.Field()


class WlwLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip)
    default_output_processor = TakeFirst()

    total_firms_in = Identity()
    site_in = MapCompose(siteBasedOnSvg, str.strip)
    email_in = MapCompose(emailBasedOnSvg, str.strip, lambda x: x[::-1])
    phone_in = MapCompose(phoneBasedOnSvg, str.strip)
    angebots_in = MapCompose(angebot, str.strip)
    angebots_out = Join(', ')
    delivery_in = MapCompose(deliveryText, str.strip)
    facts_in = MapCompose(factsText, str.strip)
    certificates_in = MapCompose(certificatesText, str.strip)
    page_in = Identity()
    totalOnPage_in = Identity()


def isStatusActive(iTag):
    if iTag.xpath('./@class').extract_first().find('disabled') < 0:
        type_ = iTag.xpath('./@title').extract_first().strip()
        if type_ == 'Hersteller':
            return 'producer'
        elif type_ == 'Dienstleister':
            return 'service'
        elif type_ == 'Händler':
            return 'distrib'
        elif type_ == 'Großhändler':
            return 'wholesaler'


class StatusItemLoader(ItemLoader):
    status_in = MapCompose(isStatusActive)
    status_out = Join(', ')


def mergeFact(liTag):
    L = liTag.xpath('.//text()').extract()
    return ' '.join(filter(lambda x: x != ' ', L))


class FactsItemLoader(ItemLoader):
    facts_in = MapCompose(mergeFact)
    facts_out = Join(', ')
