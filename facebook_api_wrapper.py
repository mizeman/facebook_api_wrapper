"""
Access the most common functions of Facebook api.

Download posts by ids, download all posts by given profile, profile infos,
all with rate limiting.
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import time
import requests
import pandas as pd
import facebook
import logging
from functools import wraps
import pytz

# TODO what error message is printed for calls for posts older than profile?


POST_FIELDS = [  # list of fields called when acquiring posts by Facebook SDK
    "id",
    "application",
    "caption",
    "created_time",
    "description",
    "from",
    "link",
    "message",
    "message_tags",
    "name",
    "object_id",
    "parent_id",
    "permalink_url",
    "picture",
    "place",
    "properties",
    "status_type",
    "story",
    "type",
    "updated_time",
    "comments.filter(stream).limit(0).summary(true)",
    "likes.limit(0).summary(true)",
    "shares",
    "reactions.summary(true)",
    ]

COMMENT_FIELDS = [
    "id",
    "comment_count",
    "created_time",
    "from",
    "like_count",
    "message",
    "message_tags",
    "object",
    "parent",
    ]

POST_INSIGHT_FIELD = "insights.metric({})".format(  # extra insights field
    ",".join([
        "post_activity_by_action_type_unique",
        "post_impressions_unique",
        "post_impressions_paid_unique",
        "post_impressions_fan_unique",
        "post_impressions_fan_paid_unique",
        "post_impressions_organic_unique",
        "post_impressions_viral_unique",
        "post_impressions_nonviral_unique",
        "post_impressions_by_story_type_unique",
        "post_engaged_users",
        "post_negative_feedback_by_type_unique",
        "post_engaged_fan",
        "post_clicks_by_type_unique",
        "post_reactions_by_type_total",
    ]))  # TEST tested last in 2019, if needed, test!


class SocialMediaApi:
    """Handles common features of Facebook and Twitter api.

    Methods
    -------
    profiles_info(ids, path=None) -> pd.DataFrame
        Returns profiles info acquired by api calls.
    posts(ids, insights=False, path=None) -> pd.DataFrame
        Returns posts acquired by api calls.
    profiles_posts(ids, since, until, n=100000, insights=False, path=None)
        Returns posts from profiles within time range acquired by api calls.
    """

    def save_df(self, df: pd.DataFrame, path: str):
        """Return DataFrame of elements and saves it to csv or excel path.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to save
        path : str
            path to table output file. Based on extension, table is saved to:
                None: nowhere
                .xlsx: Excel spreadsheet
                else: csv table
        """
        if path is None:
            pass
        elif path.endswith(".xlsx"):
            with pd.ExcelWriter(  # excel does not support timezones
                    path=path,
                    engine='xlsxwriter',
                    options={'remove_timezone': True}) as writer:
                df.to_excel(writer)
        else:
            df.to_csv(path, encoding="utf-8")

    def add_info(self, df: pd.DataFrame):
        """Return `df` with profiles info columns.

        All new columns are added to the end of `df` with 'profile_' prefix.

        Parameters
        ----------
        df : pd.DataFrame
            Posts dataframe. Must consist of self.post_from_col column.
        """
        if len(df) == 0:
            logging.warning(
                ("no posts downloaded, thus no profile info is downloaded"))
            return df
        else:
            ids_downloaded = list(df[self.post_from_col].unique())
            info_df = self.profiles_info(ids_downloaded)
            info_df = info_df.add_prefix("profile_")

            info_df.profile_api_call_id = info_df.profile_api_call_id.astype(
                "str")
            df[self.post_from_col] = df[self.post_from_col].astype("str")
            df = df.merge(
                info_df, how="left",
                left_on=self.post_from_col,
                right_on="profile_api_call_id")

            return df

    def profiles_info(self, ids: list, path: str=None) -> pd.DataFrame:
        """Return profiles info acquired by api calls.

        Parameters
        ----------
        ids : list of strs or ints
            ids or names of profiles
        path : str or None (default None)
            if passed, dataframe is saved there as csv

        Output index 'api_call_id' notes profile id/name on which api was called.
        """
        elements = []
        for i in ids:
            element = self.get_profile_info(i)
            element["api_call_id"] = i
            elements.append(element)

        df = pd.DataFrame(elements)
        self.save_df(df, path)

        return df

    def posts(
            self,
            ids: list,
            insights: bool=False,
            comments: bool=False,
            info: bool=False,
            path: str=None) -> pd.DataFrame:
        """Return posts acquired by api calls.

        Parameters
        ----------
        ids : list of strs or ints
            posts ids
        insights : bool (default False)
            whether insight fields should be called (requires page access token
            with admin rights)
        comments : bool (default False)
            whether comments_reactions should be included (demands downloading
            all comments)
        info : bool (default False)
            whether profiles info should be included (demands downloading all
            profiles info); usage includes accessing Facebook profile fans
        path : str or None (default None)
            if passed, dataframe is saved there as csv
        """
        posts = []
        for i in ids:
            element = self.get_post(i, insights=insights)
            if element is not None:
                posts.extend(self.transform_posts([element], i))

        return posts
        try:
            df = pd.DataFrame(posts)
        except AttributeError:
            print(posts)
            return posts

        if info:
            df = self.add_info(df)
        if comments:
            df = self.add_comments(df)

        self.save_df(df, path)
        return df

    def profiles_posts(
            self,
            ids: list,
            since: datetime.datetime,
            until: datetime.datetime=datetime.datetime(2100, 1, 1),
            n: int=100000,
            insights: bool=False,
            comments: bool=False,
            info: bool=False,
            path: str=None
            ) -> pd.DataFrame:
        """Return posts from profiles within time range acquired by api calls.

        Parameters
        ----------
        ids : list of strs or ints
            profiles ids or names
        since : datetime.datetime
            start of time range
        until : datetime.datetime
            end of time range
        n : int (default 100,000)
            maximal number of downloaded posts
        insights : bool (default False)
            whether insight fields should be called (requires page access token
            with admin rights)
        comments : bool (default False)
            whether comments_reactions should be included (demands downloading
            all comments)
        info : bool (default False)
            whether profiles info should be included (demands downloading all
            profiles info); usage includes accessing Facebook profile fans
        path : str (default None)
            if passed, dataframe is saved there as csv
        """
        def add_timezone(x):
            if x.tzinfo is None:
                return pytz.utc.localize(x)
            else:
                return x

        def paginate_elements(first_connection):
            connection = first_connection

            while (
                    (len(elements) < n) and
                    (pd.to_datetime(
                        self.connection_date(elements[-1])) > since_tz)):
                try:
                    connection = self.get_next_connection(connection)
                except KeyError:
                    logging.info(
                        ("connection has no next page: downloading profile "
                         "posts with parameters {}")
                        .format([i, since, until, n, insights]))
                    return elements

                if self.returns_data(connection):
                    elements.extend(self.connection_data(connection))
                else:
                    logging.error(
                        ("no data in connection {} while downloading profile "
                         "posts with parameters {}")
                        .format(connection, [i, since, until, n, insights]))
                    return elements

            return elements

        # timezone is needed for dates comparison
        since_tz = add_timezone(since)
        until_tz = add_timezone(until)

        posts = []
        for i in ids:
            elements = []

            # First, self.get_profile_posts_initial_page is called.
            connection = self.get_profile_posts_initial_call(
                i, since_tz, until_tz, n, insights)
            if self.returns_data(connection):
                elements.extend(self.connection_data(connection))

                # Afterwards, next page of results is listed until number of
                # posts or time range is exceeded.
                elements = paginate_elements(connection)
                posts.extend(self.transform_posts(elements, i))

        df = pd.DataFrame(posts)

        # erase posts outside of time range (Twitter does not allow to
        # download specific time range, but posts have to be downloaded from
        # present backwards)
        if len(df) > 0:
            try:
                df = df.loc[
                    ((df.created_time >= since) & (df.created_time <= until))]
            except AttributeError:
                logging.warning(
                    ("time range control not executed: downloaded table has "
                     "no 'created_time' column; present columns: {}")
                    .format(df.columns))

        if info:
            df = self.add_info(df)
        if comments:
            df = self.add_comments(df)

        self.save_df(df, path)
        return df


class FbApi(SocialMediaApi):
    """Handle downloading from Facebook Graph api.

    Attributes
    ----------
    token: Facebook access token
        (app access token or page access token, based on usage)

    Methods
    -------
    profiles_info(ids, path=None) -> pd.DataFrame
        Returns profiles info acquired by api calls.
    posts(ids, insights=False, path=None) -> pd.DataFrame
        Returns posts acquired by api calls.
    profiles_posts(ids, since, until, n=100000, insights=False, path=None)
        Returns posts from profiles within time range acquired by api calls.
    posts_comments(ids, n=100000, path=False) -> pd.DataFrame
        Returns comments under posts with given ids acquired by api calls.

    Examples
    --------
    >>> f = FbApi("fb_access_token")
    >>> f.profiles_info(profiles_ids)
    >>> f.posts(posts_ids)
    >>> f.profiles_posts(profiles_ids, since, until)
    >>> f.posts_comments(posts_ids)

    Notes
    -----
    Wraps up Facebook Python SDK library:
        https://github.com/mobolic/facebook-sdk
    """

    def __init__(self, token="xxx"):
        self.api = facebook.GraphAPI(token, version="3.1")

    def rate_limit_sdk(func):  # TODO max_tries and wait as parameters
        """Return `func` multiple times in case of SDK limit error.

        After each rate limited call, waits for 15 minutes and tries again
        or gives up after 2 hour limit is reached.

        Twin method for self.rate_limit_requests (only difference in error
        code location).
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            max_wait = 7200
            wait = 900

            max_tries = max_wait // wait
            tries = 0

            while tries <= max_tries:
                try:
                    return func(*args, **kwargs)
                except facebook.GraphAPIError as e:
                    if e.code == 4:
                        logging.warning(
                            "request limit reached, waiting for 15 minutes")
                        time.sleep(wait)
                    else:
                        logging.warning(
                            ("Facebook sdk returned error message while"
                             "calling {} with args: {}, kwargs: {}, error: {}")
                            .format(
                                    func.__name__,
                                    args,
                                    kwargs,
                                    e))
                        return []
                tries += 1

            logging.error(
                ("request limit not solved, downloading stopped "
                 "while calling {} with args: {}, kwargs: {}")
                .format(func.__name__, *args, **kwargs))
            return []

        return wrapper

    def rate_limit_requests(func):  # TODO max_tries and wait as parameters
        """Return `func` multiple times in case of request limit error.

        After each rate limited call, waits for 15 minutes and tries again
        or gives up after 2 hour limit is reached.

        Twin method for self.rate_limit_sdk (only difference in error
        code location).
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            max_wait = 7200
            wait = 900

            max_tries = max_wait // wait
            tries = 0

            while tries <= max_tries:
                connection = func(*args, **kwargs)
                if "error" in connection:
                    if connection["error"]["code"] == 4:
                        logging.warning(
                            "request limit reached, waiting for 15 minutes")
                        time.sleep(wait)
                    else:
                        logging.warning(
                            ("Facebook sdk returned error message while"
                             "calling {} with args: {}, kwargs: {}, \n\n"
                             "error: {}")
                            .format(
                                    func.__name__,
                                    args,
                                    kwargs,
                                    connection["error"]))
                        return []
                else:
                    return connection
                tries += 1

            logging.error(
                ("request limit not solved, downloading stopped "
                 "while calling {} with args: {}, kwargs: {}")
                .format(func.__name__, *args, **kwargs))
            return []

        return wrapper

    def returns_data(self, connection) -> bool:
        try:
            return connection["data"] != []
        except (KeyError, TypeError):
            return False

    def connection_data(self, connection):
        return connection["data"]

    def connection_date(self, connection):
        return connection["created_time"]

    def transform_posts(self, posts: list, i) -> list:
        """Return list of posts in dictionaries with additional collumns.

        Parameters
        ----------
        posts : list of dicts
            posts to transform
        i : str or int
            api call id noting how the row was acquired (ie. from profile with
            given `i` or by calling post id `i` directly)

        New columns:
            comments_count
            likes_count
            reactions_count
            shares_count
            interactions  ( = comments_count + reactions_count + shares_count)
            post_link
            from_id
            from_name
            api_call_id  ( = i)

        Updated columns:
            created_time is pd.to_datetime'd
        """
        def post_with_additional_collumns(post):
            if post == []:
                return []
            else:
                try:
                    post["comments_count"] = post["comments"][
                        "summary"]["total_count"]
                except KeyError:
                    post["comments_count"] = None
                try:
                    post["from_id"] = post["from"]["id"]
                except KeyError:
                    post["from_id"] = None
                try:
                    post["from_name"] = post["from"]["name"]
                except KeyError:
                    post["from_name"] = None

                post["created_time"] = pd.to_datetime(post["created_time"])
                post["created_time"] = post["created_time"].replace(
                    tzinfo=None)

                try:
                    post["likes_count"] = post["likes"]["summary"][
                        "total_count"]
                except KeyError:
                    try:
                        post["likes_count"] = post["like_count"]
                    except KeyError:
                        post["likes_count"] = None
                try:
                    post["reactions_count"] = post["reactions"]["summary"][
                        "total_count"]
                except KeyError:
                    post["reactions_count"] = None
                try:
                    post["shares_count"] = post.get("shares", {}).get(
                        "count", 0)
                except KeyError:
                    pass
                post["post_link"] = "https://facebook.com/{post_id}".format(
                    post_id=post["id"])

                interactions_cols = [
                    "comments_count",
                    "reactions_count",
                    "shares_count"
                    ]
                post["interactions"] = sum(
                    [post[col] for col in interactions_cols
                     if post[col] is not None])

                post["api_call_id"] = i

            return post
        return [post_with_additional_collumns(post) for post in posts]

    def transform_comments(self, comments, i):
        """Return list of comments in dictionaries with additional collumns.

        Parameters
        ----------
        comments: list of dicts
            comments to transform
        i: str or int
            api call id noting how the row was acquired (ie. from profile with
            given `i` or by calling post id `i` directly)

        New column:
            api_call_id ( = i)
        """
        def comment_with_additional_collumns(comment):
            comment["api_call_id"] = i
            return comment

        comments = [comment_with_additional_collumns(c) for c in comments]
        return comments

    @rate_limit_sdk
    def get_profile_info(self, i) -> dict:
        """Return profile info api call.

        Parameters
        ----------
        i : str or int
            profile id or name
        """
        return self.api.get_connections(
            i,
            "?fields=id,fan_count,username,link,name")

    @rate_limit_sdk
    def get_post(self, i, insights: bool) -> dict:
        """Return post api call.

        Parameters
        ----------
        i : str or int
            post id
        insights : bool
            whether insight fields should be called (requires page access token
            with admin rights)
        """
        fields = ",".join(POST_FIELDS)
        if insights:
            fields += ",{}".format(POST_INSIGHT_FIELD)
        return self.api.get_object(i, fields=fields)

    @rate_limit_sdk
    def get_profile_posts_initial_call(
            self,
            i,
            since: datetime.datetime,
            until: datetime.datetime,
            n: int,
            insights: bool) -> dict:
        """Call api for the first profile posts call.

        Additional calls are done by self.get_next_connection.

        Parameters
        ----------
        i : str or int
            profile id or name
        since : datetime.datetime
            start of time range
        until : datetime.datetime
            end of time range
        n : int
            maximal number of downloaded posts
        insights : bool
            whether insight fields should be called (requires page access token
            with admin rights)
        """
        max_n_first = 25

        fields = ",".join(POST_FIELDS)
        if insights:
            fields += ",{}".format(POST_INSIGHT_FIELD)

        n_first = min(n, max_n_first)
        since_secs = int(time.mktime(since.timetuple()))
        until_secs = int(time.mktime(until.timetuple()))
        fields += f"&limit={n_first}&since={since_secs}&until={until_secs}"

        return self.api.get_connections(
            i,
            "posts?fields={0}".format(fields))

    @rate_limit_sdk
    def get_post_comments_initial_call(
            self,
            i: str,
            n: int
            ) -> dict:
        """Call api for the first post comments call.

        Additional calls are done by self.get_next_connection.

        Parameters
        ----------
        i : str
            post id
        n : int
            maximal number of downloaded posts
        """
        fields = ",".join(COMMENT_FIELDS)

        return self.api.get_connections(
            i,
            "comments?fields={0}".format(fields))

    @rate_limit_requests
    def get_next_connection(self, connection: dict) -> dict:
        """Return next page of connection.

        Parameters
        ----------
        connection: dict
            previously downloaded page by requests api call
        """
        return requests.get(connection["paging"]["next"]).json()

    def add_comments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return `df` with 'comments_reactions' col.

        Comments reactions note the total number of reactions for all post
        comments.

        Parameters
        ----------
        df: pd.DataFrame
            Posts dataframe. Must consist of "id" column.
        """
        posts_ids = list(df.id.unique())
        comments_df = self.posts_comments(posts_ids)
        comments_reactions = comments_df.groupby("api_call_id"
                                                 ).like_count.sum()
        df["comments_reactions"] = df.id.map(comments_reactions.to_dict())
        df.comments_reactions = df.comments_reactions.fillna(0)

        return df

    def posts_comments(
            self,
            ids: list,
            n: int=100000,
            path: str=None
            ) -> pd.DataFrame:
        """Return comments under posts with given ids acquired by api calls.

        Parameters
        ----------
        ids : list of ints
            posts ids
        n : int
            maximal number of downloaded posts
        path : str
            if passed, dataframe is saved there as csv
        """
        def paginate_elements(first_connection):
            connection = first_connection

            while len(elements) < n:
                try:
                    connection = self.get_next_connection(connection)
                except KeyError:
                    logging.info(
                        ("connection has no next page: downloading comments "
                         "to post with parameters {}")
                        .format([i, n]))
                    return elements

                if self.returns_data(connection):
                    elements.extend(self.connection_data(connection))
                else:
                    logging.error(
                        ("no data in connection {} while downloading comments "
                         "to posts with parameters {}")
                        .format(connection, [i, n]))
                    return elements

        comments = []
        for i in ids:
            print(i)
            elements = []
            connection = self.get_post_comments_initial_call(i, n)
            if self.returns_data(connection):
                elements.extend(self.connection_data(connection))
                elements.extend(paginate_elements(connection))
                comments.extend(self.transform_comments(elements, i))

        df = pd.DataFrame(comments)
        self.save_df(df, path)
        return df
