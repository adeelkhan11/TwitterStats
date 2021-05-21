from sqlalchemy import Column, Integer, String, DECIMAL, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
import datetime
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Trend(Base):
    __tablename__ = 'trend'

    date = Column(DateTime, primary_key=True)
    trend = Column(String(100), primary_key=True)
    score = Column(Integer)
    tweet_count = Column(Integer)
    tweep_count = Column(Integer)
    updated_date = Column(DateTime, onupdate=datetime.datetime.now, default=datetime.datetime.now)


class Mention(Base):
    __tablename__ = 'mention'

    date = Column(DateTime, primary_key=True)
    screen_name = Column(String(100), primary_key=True)
    name = Column(String(100))
    image = Column(String(250))
    score = Column(Integer)
    updated_date = Column(DateTime, onupdate=datetime.datetime.now, default=datetime.datetime.now)


class Tweep(Base):
    __tablename__ = 'tweep'

    date = Column(DateTime, primary_key=True)
    trend = Column(String(100), primary_key=True)
    screen_name = Column(String(100), primary_key=True)
    name = Column(String(100))
    image = Column(String(250))
    score = Column(Integer)
    tweets_posted = Column(Integer)
    rts_posted = Column(Integer)
    rts_received = Column(Integer)
    botness = Column(Integer)
    updated_date = Column(DateTime, onupdate=datetime.datetime.now, default=datetime.datetime.now)


def get_session(date: datetime):
    engine = create_engine(f'sqlite:///data/publish_{date.year}.db')

    # Create all tables in the engine. This is equivalent to "Create Table"
    # statements in raw SQL.
    Base.metadata.create_all(engine)

    DBSession = sessionmaker(bind=engine)

    session = DBSession()

    return session


def main():
    engine = create_engine(f'sqlite:///../data/publish_{datetime.datetime.now().year}.db')

    # Create all tables in the engine. This is equivalent to "Create Table"
    # statements in raw SQL.
    Base.metadata.create_all(engine)

    # DBSession = sessionmaker(bind=engine)

    # session = DBSession()

    # session.commit()


if __name__ == '__main__':
    main()
