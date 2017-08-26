# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join, Identity

# 1. Name and address data


def siteBasedOnSvg(svg):
    if svg.extract().find('"#svg-icon-website"') >= 0:
        return svg.xpath('./ancestor::a[1]/@href').extract_first()


def emailBasedOnSvg(svg):
    if svg.extract().find('"#svg-icon-email"') >= 0:
        return svg.xpath('./ancestor::a[1]//text()').extract_first()


def phoneBasedOnSvg(svg):
    if svg.extract().find('"#svg-icon-earphone"') >= 0:
        return svg.xpath('./ancestor::a[1]/@data-content').extract_first()

# 2. Delivery, daten und fakten, zertificates


def deliveryText(factTag):
    if factTag.xpath('./div[1]//text()'
                     ).extract_first().strip() == 'Liefergebiet':
        return factTag.xpath('./div[2]//text()').extract_first()


class FactsItem(scrapy.Item):
    facts = scrapy.Field()


def mergeFact(liTag):
    L = liTag.xpath('.//text()').extract()
    return ' '.join(filter(lambda x: x != ' ', L))


class FactsItemLoader(ItemLoader):
    facts_in = MapCompose(mergeFact)
    facts_out = Join(', ')


def factsText(factTag):
    if factTag.xpath('./div[1]//text()'
                     ).extract_first().strip() == 'Daten und Fakten':
        huj = factTag.xpath('./div[2]')
        factsLoader = FactsItemLoader(item=FactsItem(), selector=huj)
        lis = factsLoader.nested_xpath('.//li')
        lis.add_value('facts', lis.selector)
        fi = factsLoader.load_item()
        return fi['facts']


def certificatesText(factTag):
    if factTag.xpath('./div[1]//text()'
                     ).extract_first().strip() == 'Zertifikate':
        l = factTag.xpath('./div[2]//text()').extract()
        return ', '.join(filter(lambda x: x not in [' ', ''], l))

# 3. Uber uns section


def aboutText(uberUnsTag):
    if uberUnsTag.xpath('./div[1]//text()'
                        ).extract_first().strip() == 'Unternehmen':
        return uberUnsTag.xpath('./div[2]//text()').extract_first()


def peopleText(uberUnsTag):
    if uberUnsTag.xpath('./div[1]//text()'
                        ).extract_first().strip() == 'Leitende Mitarbeiter':
        rows = uberUnsTag.xpath('.//tr')
        output = []
        for row in rows:
            x = ' '.join(map(lambda x: x.strip(),
                             row.xpath('.//text()').extract()
                             )
                         )
            output.append(x)
        return ', '.join(output)

# 4. Ansprechpartner


def sprechText(sprechTag):
    txt = sprechTag.xpath('.//text()').extract()
    if len(txt) >= 2:
        if any(word in txt[1] for word in ['Herr', 'Frau']):
            return txt[1].strip()


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


class WlwBaseItem(scrapy.Item):
    firmaId = scrapy.Field()
    nameInUrl = scrapy.Field()
    page = scrapy.Field()
    linksGot = scrapy.Field()
    isDuplicate = scrapy.Field()


class WlwItem(WlwBaseItem):
    # 1. Name and address data
    name = scrapy.Field()
    full_addr = scrapy.Field()
    site = scrapy.Field()
    email = scrapy.Field()
    phone = scrapy.Field()
    street = scrapy.Field()
    building = scrapy.Field()
    zip = scrapy.Field()
    city = scrapy.Field()
    # 2. Delivery, daten und fakten, zertificates
    delivery = scrapy.Field()
    facts = scrapy.Field()
    certificates = scrapy.Field()
    # 3. Uber uns section
    about = scrapy.Field()
    key_people = scrapy.Field()
    # 4. Ansprechpartner
    common_person = scrapy.Field()
    # 5. Categories, bilatt
    angebots = scrapy.Field()


class WlwLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip)
    default_output_processor = TakeFirst()

    # 1. Name and address data
    site_in = MapCompose(siteBasedOnSvg, str.strip)
    email_in = MapCompose(emailBasedOnSvg, str.strip, lambda x: x[::-1])
    phone_in = MapCompose(phoneBasedOnSvg, str.strip)
    # 2. Delivery, daten und fakten, zertificates
    delivery_in = MapCompose(deliveryText, str.strip)
    facts_in = MapCompose(factsText, str.strip)
    certificates_in = MapCompose(certificatesText, str.strip)
    # 3. Uber uns section
    about_in = MapCompose(aboutText, str.strip)
    key_people_in = MapCompose(peopleText)
    # 4. Ansprechpartner
    common_person_in = MapCompose(sprechText, str.strip)


    angebots_in = Identity()
    # angebots_out = Join(', ')


class AngebotItem(scrapy.Item):
    name = scrapy.Field()


class AngebotLoader(ItemLoader):
    default_output_processor = TakeFirst()


class StatusItem(scrapy.Item):
    status = scrapy.Field()


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



