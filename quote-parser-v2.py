from fake_useragent import UserAgent
import lxml.html as html
import pandas as pd
import requests
import time
import concurrent.futures


def get_page_by_url(url, user_agent_gen):
    header = {'User-Agent': str(user_agent_gen.random)}
    r = requests.get(url, headers=header)
    page = html.fromstring(r.text)
    return page


def dump_data(filename, result):
    df1 = pd.DataFrame({'Tag': list(result['tags'].keys()),
                        'Url': list(result['tags'].values())})

    df2 = pd.DataFrame(result['quotes'], columns=['Author', 'Quote', 'Tags'])
    df3 = pd.DataFrame(result['authors'], columns=['Author', 'Born', 'Location', 'Description', 'url'])

    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    df1.to_excel(writer, sheet_name='tags', index=False)
    df2.to_excel(writer, sheet_name='quotes', index=False)
    df3.to_excel(writer, sheet_name='authors', index=False)
    writer.save()


def parse_tags(tags, result):
    keywords = []
    for tag in tags:
        keywords.append(tag.text)
        result[tag.text] = tag.attrib['href']
    return ','.join(keywords)


def get_text(elem, class_name):
    if elem is not None:
        item = elem.find_class(class_name).pop()
        if item is not None:
            return item.text.strip()


def parse_author(lnk, user_agent_gen):
    page = get_page_by_url(lnk, user_agent_gen)
    detail = page.find_class('author-details').pop()
    author_name = get_text(detail, 'author-title')
    author_lnk = lnk
    author_born = get_text(detail, 'author-born-date')
    author_location = get_text(detail, 'author-born-location')
    author_description = get_text(detail, 'author-description')

    return author_name, author_born, author_location, author_description, author_lnk


def check_author_lnk(authors, lnk):
    for record in authors:
        if record[4] == lnk:
            return True
    return False


def parse(base_url, user_agent_gen, page_num):
    result = dict()
    result['tags'] = {}
    result['quotes'] = []
    result['authors'] = []

    url = f"{base_url}/page/{page_num}/"
    page = get_page_by_url(url, user_agent_gen)
    quoutes = page.xpath('//div[@class="quote"]')
    if not quoutes:
        return None

    for quote in quoutes:
        author_title = None
        quote_text = None
        tags = quote.find_class('tag')
        keywords = parse_tags(tags, result['tags'])
        text = quote.find_class('text')
        if text:
            quote_text = text[0].text

        author = quote.find_class('author')
        if author:
            author_title = author[0].text

        spans = quote.findall('span')
        if spans:
            span = spans[-1]
            link = span.find('a')
            if link is not None:
                if link.text == '(about)':
                    author_url = f"{base_url}{link.attrib['href']}"
                    if not check_author_lnk(result['authors'], author_url):
                        result['authors'].append(parse_author(author_url, user_agent_gen))
        result['quotes'].append((author_title, quote_text, keywords))

    return result


def prepare_data(data):
    result = dict()
    result['tags'] = {}
    result['quotes'] = []
    result['authors'] = []
    for chunk in data:
        tags = chunk['tags']
        for tag in tags.keys():
            if tag not in result['tags']:
                result['tags'][tag] = tags[tag]

        result['quotes'].extend(chunk['quotes'])
        for item in chunk['authors']:
            flag = False
            item_author, *tail = item
            for author in result['authors']:
                author_title, *tail = author
                if author_title == item_author:
                    flag = True
                    break
            if not flag:
                result['authors'].append(item)

    return result


def workder(page):
    res = parse('https://quotes.toscrape.com', ua, page)
    return res


if __name__ == '__main__':
    start_time = time.time()
    CONNECTIONS = 10
    ua = UserAgent()
    data = []
    pages = [n for n in range(1, 11)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        res = executor.map(workder, pages)

    data = [x for x in list(res) if x] # чистим список от None
    print("Parse data: %s seconds" % (time.time() - start_time))
    result = prepare_data(data)
    print("Prepare data: %s seconds" % (time.time() - start_time))
    dump_data('dump-data2.xlsx', result)
    print("Save data: %s seconds" % (time.time() - start_time))

