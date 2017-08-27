# -*- coding: utf-8 -*-
import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import (TakeFirst, MapCompose, Join, Identity,
                                      Compose)

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

# 5. Categories, bilatt = see AngebotItem below


class WlwBaseItem(scrapy.Item):
    firmaId = scrapy.Field()
    nameInUrl = scrapy.Field()
    page = scrapy.Field()
    linksGot = scrapy.Field()
    isDuplicate = scrapy.Field()
    timestamp = scrapy.Field()
    source = scrapy.Field()


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
    # 5. Categories, bilatt (see AngebotItem below)
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
    # 5. Categories, bilatt
    angebots_in = Identity()
    angebots_out = Identity()


# ************************************************************
# 5. Categories, bilatt
def offerText(liTag):
    p = liTag.xpath('p')
    if p:
        return p.xpath('text()').extract_first()


def person(liTag):
    if liTag.xpath('i'):
        return liTag.xpath('text()').extract_first()


def phone(liTag):
    a = liTag.xpath('a')
    if a:
        txt = a.extract_first()
        if 'xlink:href="#svg-icon-earphone"' in txt:
            return a.xpath('@data-content').extract_first()


def email(liTag):
    a = liTag.xpath('a')
    if a:
        txt = a.extract_first()
        if 'xlink:href="#svg-icon-email"' in txt:
            return a.xpath('.//text()').extract_first()


def nameInUrl(url):
    splitt = url.rsplit('/', 1)
    if len(splitt) == 2:
        part = splitt[1].split('?')
        return part[0]



class AngebotItem(scrapy.Item):
    listing_id = scrapy.Field()
    caption = scrapy.Field()
    is_producer = scrapy.Field()
    is_service = scrapy.Field()
    is_distrib = scrapy.Field()
    is_wholesaler = scrapy.Field()
    offer_text = scrapy.Field()
    contact_person = scrapy.Field()
    phone = scrapy.Field()
    email = scrapy.Field()
    cat_id = scrapy.Field()
    nameinurl = scrapy.Field()


class AngebotLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip)
    default_output_processor = TakeFirst()

    listing_id_in = Identity()
    is_producer_in = MapCompose(lambda x: 0 if 'disabled' in x else 1)
    is_service_in = MapCompose(lambda x: 0 if 'disabled' in x else 1)
    is_distrib_in = MapCompose(lambda x: 0 if 'disabled' in x else 1)
    is_wholesaler_in = MapCompose(lambda x: 0 if 'disabled' in x else 1)
    offer_text_in = MapCompose(offerText, str.strip)
    contact_person_in = MapCompose(person, str.strip)
    phone_in = MapCompose(phone, str.strip)
    email_in = MapCompose(email, str.strip, lambda x: x[::-1])
    cat_id_in = MapCompose(str.strip, lambda x: int(x))
    nameinurl_in = MapCompose(nameInUrl, str.strip)
