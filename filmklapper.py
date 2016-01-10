import argparse
import configparser
import datetime
import queue
import re
import smtplib
import threading
import urllib

import goslate
import pytz
import requests

from lxml import html as lh
from requests import RequestException, HTTPError


SKIP_SPECIAL = ['PathéOpera', 'PathéArt', 'PathéBallet', 'PathéOperaEncore', 'PathéTheatre']
PATHE_TECHNOLOGIES = ['3D', 'IMAX', 'IMAX3D']
DAYS_OF_WEEK = ['maandag', 'dinsdag', 'woensdag', 'donderdag', 'vrijdag', 'zaterdag', 'zondag']
MONTHS_SHORT = ['jan', 'feb', 'mrt', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec']
MONTHS_FULL = [
    'januari', 'februari', 'maart', 'april', 'mei', 'juni', 'juli', 'augustus', 'september',
    'oktober', 'november', 'december'
]

TODAY = datetime.date.today()
TOMORROW = TODAY + datetime.timedelta(days=1)


class IMDBMovieNotFoundError(Exception):
    pass


class PatheMovieParseError(Exception):
    pass


def nl_to_en(text):
    """Translate the given text from Dutch to English.

    :param str text: Dutch text to be translated
    :return: English text
    :rtype: str
    """
    gs = goslate.Goslate()
    try:
        return gs.translate(text, source_language='nl', target_language='en')
    except urllib.error.HTTPError:
        return text


# http://stackoverflow.com/a/6558571/695332
def next_weekday(weekday):
    """Get the date of the next weekday.

    :param int weekday: weekday as a numeric index (0 - Monday, 6 - Sunday)
    :return: date of the next occurrence of the weekday
    :rtype: datetime.date
    """
    days_ahead = weekday - TODAY.weekday()
    if days_ahead <= 0:  # target day already happened this week
        days_ahead += 7
    return TODAY + datetime.timedelta(days_ahead)


def get_movie_special(movie_page_doc):
    """Get Pathe movie special status.

    Pathe special movie statuses are PathéOpera, PathéArt, PathéBallet, PathéOperaEncore,
    PathéTheatre, PatheMusic, PathéDOCS, PAC.
    :param etree.Element movie_page_doc: movie page DOM model
    :return: movie special status if it exists, None otherwise
    :rtype: str
    """
    special_string = movie_page_doc.xpath(
        'string(//div[@class="moviedetail-side"]/ul/li/span[text() = "Special:"]/..)'
    )
    if special_string:
        special_string = ''.join(special_string.split())
        special = special_string.split(':')[1]
        return special


def get_movie_title(movie_page_doc):
    """Get Pathe movie title.

    The title retrieved from the movie page DOM model is normalized by removing any additional info
    added by Pathe (language/subtitle versions, age restriction, film festival tags etc.) so it
    becomes possible to search for this title on IMDB.
    This is prone to bugs. E.g. "Wo Hu Cang Long (Crouching Tiger, Hidden Dragon)" will cut off a
    valid title part (though this movie will still be found on IMDB).
    :param etree.Element movie_page_doc: movie page DOM model
    :return: movie title
    :rtype: str
    """
    title = (
        movie_page_doc.xpath('string(//h1[@itemprop="name"])') or
        movie_page_doc.xpath('string(//div[@class="page-title "]/div[@class="page-cell"]/h1)')
    )
    title_match = re.match('^(.*)\s(\(.*\))$', title)
    if title_match:
        title = title_match.group(1)
    return title


def get_movie_release_date(movie_page_doc):
    """Get Pathe movie release date.

    :param etree.Element movie_page_doc: movie page DOM model
    :return: movie release date
    :rtype: datetime.date
    """
    date_string = movie_page_doc.xpath('string(//span[@class="release-date"]/em)')
    date_string_match = re.match('^(\d{1,2})\s([a-z]+)\s(\d{4})', date_string)
    groups = date_string_match.groups()
    day = groups[0]
    month = MONTHS_FULL.index(groups[1]) + 1
    year = groups[2]
    date_string = '{0}.{1}.{2}'.format(day, month, year)
    return datetime.datetime.strptime(date_string, '%d.%m.%Y').date()


def get_movie_genres(movie_page_doc):
    """Get Pathe movie genres.

    :param etree.Element movie_page_doc: movie page DOM model
    :return: movie genres if found, None otherwise
    :rtype: list
    """
    genres_string = movie_page_doc.xpath(
        'string(//div[@class="moviedetail-side"]/ul/li/span[text()="Genre:"]/..)'
    )
    if genres_string:
        genres_string = ''.join(genres_string.split())
        genres = genres_string.split(':')[1].split(',')
        genres = [nl_to_en(genre) for genre in genres]
        return genres


def get_movie_duration(movie_page_doc):
    """Get Pathe movie duration.

    :param etree.Element movie_page_doc: movie page DOM model
    :return: movie duration if found, None otherwise
    :rtype: int
    """
    duration_string = movie_page_doc.xpath(
        'string(//div[@class="moviedetail-side"]/ul/li/span[text() = "Duur:"]/..)'
    )
    duration_string = duration_string.strip()
    duration_string_match = re.match('^Duur:\s+(\d+)\s+minuten$', duration_string)
    if duration_string_match:
        duration = duration_string_match.groups()[0]
        return duration


def get_movie_language(movie_page_doc):
    """Get Pathe movie language.

    :param etree.Element movie_page_doc: movie page DOM model
    :return: movie language if found, None otherwise
    :rtype: str
    """
    language_string = movie_page_doc.xpath(
        'string(//div[@class="moviedetail-side"]/ul/li/span[text()="Taalversie:"]/..)'
    )
    if language_string:
        language_dutch = language_string.strip().split(': ')[1]
        language = nl_to_en(language_dutch)
        return language


def get_movie_restrictions(movie_page_doc):
    """Get Pathe movie restrictions.

    :param etree.Element movie_page_doc: movie page DOM model
    :return: movie restrictions
    :rtype: list
    :raise: :class:~.`PatheMovieParseError` if restrictions string retrieved from the DOM model could
        not be parsed
    """
    restrictions_list = movie_page_doc.xpath(
        '//div[@class="moviedetail-side"]/ul/li/span[text()="Kijkwijzer:"]/../a/img/@src'
    )
    restrictions = []
    for restriction in restrictions_list:
        restriction_match = re.match('^/themes/main/gfx/icons/kijkwijzer/(.*).png$', restriction)
        if not restriction_match:
            continue
        restriction_name = restriction_match.groups()[0]
        if restriction_name == 'rating-onbekend-z':
            restriction_name = 'unknown'
        elif restriction_name == 'rating-nvt-z':
            restriction_name = 'n/a'
        else:
            if 'kijkwijzer-' in restriction_name:
                restriction_name = restriction_name.split('kijkwijzer-')[1]
            else:
                raise PatheMovieParseError("Unexpected restriction name")
        restrictions.append(restriction_name)
    return restrictions


def get_movie_technologies(movie_page_doc):
    """Get Pathe movie technologies.

    :param etree.Element movie_page_doc: movie page DOM model
    :return: movie technologies if found, None otherwise
    :rtype: list
    """
    technologies_string = movie_page_doc.xpath(
        'string(//div[@class="moviedetail-side"]/ul/li/span[text()="Te zien in:"]/..)'
    )
    if technologies_string:
        technologies_string = ''.join(technologies_string.split())
        technologies = technologies_string.split(':')[1].split(',')
        return technologies


def get_movie_rating(movie_page_doc):
    """Get Pathe movie rating (number of stars)

    :param etree.Element movie_page_doc: movie page DOM model
    :return: movie rating if found, None otherwise
    :rtype: int
    """
    rating_string = movie_page_doc.xpath('string(//div[@itemprop="aggregateRating"]/span/span)')
    if rating_string:
        rating_string = rating_string.replace(',', '.')
        return float(rating_string)


def get_movie_directors_cast(movie_page_doc):
    """Get Pathe movie director(s) and cast.

    :param etree.Element movie_page_doc: movie page DOM model
    :return: a tuple containing two lists - movie directors and movie cast
    :rtype: tuple
    """
    directors_cast_items = movie_page_doc.xpath('//div[@class="slider-entry"]')
    directors = []
    cast = []
    for person in directors_cast_items:
        person_name = person.xpath('span/text()')[0]
        if person.xpath('div[@class="slider-photo"]/em[text()="Regisseur"]'):
            directors.append(person_name)
        else:
            cast.append(person_name)
    return directors, cast


def do_imdb_request(path, params=None):
    """Make a request to IMDB website.

    :param str path: url path to perform the request to (url part right after the domain name)
    :param dict params: url query string parameters to insert into the url
    :return: response text
    :rtype: str
    :raise: :class:`RuntimeError` if the request failed
    """
    url = 'http://www.imdb.com/{}'.format(path)
    try:
        response = requests.get(url, params=params, headers={'Accept-Language': 'en-US,en'})
        response.raise_for_status()
    except (RequestException, HTTPError) as e:
        raise RuntimeError(
            "IMDB HTTP request failed", e, url, params
        )
    else:
        return response.text


def normalize_date(day_name):
    """Normalizes date as represented by Pathe and translates it into a :class:`datetime.date`
    object.

    Dates given by Pathe are either only the Dutch day name (e.g. vandagg, morgen, maandag, vrijdag)
    which means that the date is the next occurrence of that day, or day name plus exact date (e.g
    woensdag 27 mei).
    :param str day_name: dutch day name
    :return: date object
    :rtype: :class:`datetime.date`
    """
    if day_name == 'vandaag':
        showtime_date = TODAY
    elif day_name == 'morgen':
        showtime_date = TOMORROW
    elif day_name in DAYS_OF_WEEK:
        showtime_date = next_weekday(DAYS_OF_WEEK.index(day_name))
    else:
        precise_date = re.match('^.*\s(\d{2})\s([a-z]{3})$', day_name)

        if not precise_date:
            # No match means we encountered something unexpected
            raise PatheMovieParseError("Unexpected day name")

        # Format Pathe date representation in a form strptime can read
        match_groups = precise_date.groups()
        day = match_groups[0]
        month = MONTHS_SHORT.index(match_groups[1]) + 1
        if month < 10:
            month = '0{0}'.format(month)
        year = datetime.datetime.now().year
        date_str = '{0}.{1}.{2}'.format(day, month, year)
        showtime_date = datetime.datetime.strptime(date_str, '%d.%m.%Y')
        if showtime_date < datetime.datetime.now():
            year += 1
            showtime_date = datetime.datetime.strptime(date_str, '%d.%m.%Y')
    return showtime_date


def datetime_to_utc(dt):
    """Convert datetime in from local (Amsterdam) timezone to UTC.

    Also removes `tzinfo` attribute from the resulting datetime object so that SQLAlchemy could deal
    with it.
    :param `datetime.datetime` dt: datetime object
    :return: datetime object converted to UTC timezone
    :rtype: `datetime.datetime`
    """
    return pytz.timezone('Europe/Amsterdam').localize(dt).astimezone(pytz.UTC)


def get_movie_cinemas(movie_page_doc):
    movie_cinemas = movie_page_doc.xpath(
        '//section[@id="ScheduleContainer"]/section[@id="MovieScheduleDetails"]'
        '/section/div/div/table/@id'
    )
    return [cinema.split('_')[1] for cinema in movie_cinemas]


def get_movie_showtimes_for_cinema(movie_page_doc, cinema):
    """Get movie showtimes for cinema.

    Given the cinema name as a lowercase string (e.g. arena, delft, rembrandt) gets all the current
    movie showtimes for this cinema. Returns a list of two-value tuples representing showtime date
    and time as `datetime.datetime` object and showtime technology.
    :param etree.Element movie_page_doc: movie page DOM model
    :param str cinema: cinema name, all lowercase
    :return: list of tuples
    :rtype: list
    """
    cinema_movie_showtimes = []
    showtime_days = movie_page_doc.xpath('//table[@id="Schedule_{0}"]/tr'.format(cinema))
    for showtime_day in showtime_days:
        day_name = showtime_day.xpath('th/text()')

        # Skip elements that are not day names (i.e. that contain only whitespace)
        if len(day_name) > 1:
            continue

        day_name = day_name[0]
        showtime_date = normalize_date(day_name)

        showtimes = showtime_day.xpath('td/a')
        for showtime in showtimes:
            showtime_url = showtime.xpath('string(@href)')
            # We don't need the showtime that has already been sold out
            if showtime_url == '#modal-soldout':
                continue
            if 'javascript:openPopup' in showtime_url:
                showtime_id = re.match(
                    "^javascript:openPopup\('https://onlinetickets.pathe.nl/ticketweb.php?"
                    ".*&ShowID=(\d+)&.*'\)$",
                    showtime_url
                ).group(1)
            else:
                showtime_id = re.match('^/tickets/start/(\d+)$', showtime_url).group(1)
            showtime_id = int(showtime_id)

            showtime_items = showtime.xpath('span/text()')
            showtime_items = [''.join(showtime_item.split()) for showtime_item in showtime_items]

            # Pathe does not explicitly indicate 2D technology so append it
            if len(showtime_items) < 2:
                showtime_items.append('2D')

            # TODO: Handle date for Nacht22op23mei correctly
            # Some shows have additional info, e.g. 'Nacht22op23mei', 'Grotezaal'. We don't need it
            # so remove it and instead replace it with a technology
            if showtime_items[1] not in PATHE_TECHNOLOGIES:
                showtime_items[1] = '2D'

            showtime_time_match = re.match('^(\d{2}:\d{2})(.*)$', showtime_items[0])
            showtime_time_groups = showtime_time_match.groups()
            if showtime_time_groups[1]:
                # TODO: Log this and send a rollbar event
                print("Something special about this showtime: {0}".format(showtime_time_groups[1]))

            showtime_time = datetime.datetime.strptime(showtime_time_groups[0], '%H:%M').time()
            showtime_datetime = datetime.datetime.combine(showtime_date, showtime_time)
            showtime_datetime = datetime_to_utc(showtime_datetime)
            cinema_movie_showtimes.append((showtime_id, showtime_datetime, showtime_items[1]))

    return cinema_movie_showtimes


def get_imdb_id_by_title(title):
    """Get IMDB movie ID given its title.

    :param title: movie title
    :return: IMDB movie ID
    :rtype: int
    """
    search_results = do_imdb_request(
        path='find',
        params={'q': title, 's': 'tt', 'ttype': 'ft'}
    )
    lh_doc = lh.fromstring(search_results)
    results = lh_doc.xpath(
        '//div[@class="findSection"]/h3[@class="findSectionHeader" and text()="Titles"]'
    )
    if not results:
        raise IMDBMovieNotFoundError("Movie not found on IMDB")

    movie = results[0]
    movie_link = movie.xpath(
        'string(../table/tr[@class="findResult odd"][1]/td[@class="result_text"]/a/@href)'
    )
    imdb_id = re.match('^/title/tt(\d+)/\?ref_=fn_ft_tt_1$', movie_link).group(1)
    imdb_id = int(imdb_id)
    return imdb_id


def get_movie_imdb_rating(**kwargs):
    """Retrieve IMDB rating for a movie.

    Given either a title or IMDB ID gets the IMDB rating for a movie.
    :param dict kwargs: title - movie title to look for on IMDB; id - movie's IMDB ID
    :return: movie's IMDB ID and IMDB rating
    :rtype: tuple
    """
    if 'title' in kwargs:
        try:
            imdb_id = get_imdb_id_by_title(kwargs['title'])
        except IMDBMovieNotFoundError:
            imdb_id = get_imdb_id_by_title(nl_to_en(kwargs['title']))
    elif 'id' in kwargs:
        imdb_id = kwargs['id']
    else:
        raise RuntimeError("Either title or ID must be provided as a search term")

    movie_page = do_imdb_request(path='title/tt{}'.format(imdb_id))
    lh_doc = lh.fromstring(movie_page)

    imdb_rating = lh_doc.xpath('//span[@itemprop="ratingValue"]/text()')
    imdb_rating = float(imdb_rating[0].strip()) if imdb_rating else None

    return imdb_id, imdb_rating


def process_movies_list_page():
    while True:
        url = pages_queue.get()
        if not url:
            break
        movies_list_page = requests.get(url)
        lh_movies_doc = lh.fromstring(movies_list_page.text)
        movies_list = lh_movies_doc.xpath(
            '//section[@id="movies-overview"]/div[contains(@class, "poster")]'
        )
        for movie in movies_list:
            if movie.xpath(
                'div/a[@class="btn btn-type-01 is-small overlay-btn"]/span/text()'
            )[0] not in ('Tijden en tickets', 'Verwacht'):
                continue

            movie_url = movie.xpath('div/a[@class="overview-overlay"]/@href')[0]
            if movie_url == '/film/845/sneak-preview':
                continue

            movie_url = 'http://www.pathe.nl' + movie_url
            movies_queue.put(movie_url)
        pages_queue.task_done()


def process_movie():
    while True:
        url = movies_queue.get()
        if not url:
            break
        movie_url_match = re.match('^http://www.pathe.nl/film/(\d+)/(.*)$', url)
        pathe_id, url_code = movie_url_match.groups()
        movie = requests.get(url)
        lh_movie_doc = lh.fromstring(movie.text)
        specials = []
        special = get_movie_special(lh_movie_doc)
        if special not in specials:
            specials.append(special)
        if special in SKIP_SPECIAL:
            movies_queue.task_done()
            continue

        title = get_movie_title(lh_movie_doc)

        try:
            imdb_id, imdb_rating = get_movie_imdb_rating(title=title)
        except IMDBMovieNotFoundError:
            imdb_id, imdb_rating = None, None
        if imdb_rating and imdb_rating > 8:
            print(url, 'http://imdb.com/title/tt{0}'.format(imdb_id))
            result_list.append((url, 'http://imdb.com/title/tt{0}'.format(imdb_id)))

        movies_queue.task_done()


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', '--config', required=True, help="Config file")
    args = arg_parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)
    mail_from = config.get('mail', 'from')
    mail_to = config.get('mail', 'to')
    smtp_host = config.get('mail', 'host')
    smtp_username = config.get('mail', 'username')
    smtp_password = config.get('mail', 'password')

    movies = requests.get('http://www.pathe.nl/films')
    lh_doc = lh.fromstring(movies.text)
    pages = lh_doc.xpath('//div[@id="pagination"]/a/text()')
    num_pages = pages[-2]

    num_page_threads = 10
    num_movie_threads = 18
    page_threads = []
    movie_threads = []

    result_list = []

    pages_queue = queue.Queue()
    movies_queue = queue.Queue()

    for _ in range(num_page_threads):
        page_thread = threading.Thread(target=process_movies_list_page)
        page_thread.start()
        page_threads.append(page_thread)

    for _ in range(num_movie_threads):
        movie_thread = threading.Thread(target=process_movie)
        movie_thread.start()
        movie_threads.append(movie_thread)

    for page_num in range(1, int(num_pages) + 1):
        movies_url = 'http://www.pathe.nl/films?page={0}'.format(page_num)
        pages_queue.put(movies_url)

    pages_queue.join()
    for _ in range(num_page_threads):
        pages_queue.put(None)
    for t in page_threads:
        t.join()

    movies_queue.join()
    for _ in range(num_movie_threads):
        movies_queue.put(None)
    for t in movie_threads:
        t.join()

    email_msg = """From: {mail_from}
To: {mail_to}
Subject: New movies in Pathe
Content-type: text/plain; charset=utf-8

""".format(mail_from=mail_from, mail_to=mail_to)

    for movie_url, imdb_url in result_list:
        email_msg += '{0} {1}\r\n'.format(movie_url, imdb_url)

    smtp_conn = smtplib.SMTP_SSL(smtp_host)
    smtp_conn.login(smtp_username, smtp_password)
    smtp_conn.sendmail(mail_from, mail_to, email_msg)
