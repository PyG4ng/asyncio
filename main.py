import asyncio
import datetime

import aiohttp
from sqlalchemy import Integer, Column, JSON
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.session import sessionmaker

api_url = 'https://swapi.dev/api/people/'

PG_DSN = 'postgresql+asyncpg://django:django@localhost:5432/swapi'
engine = create_async_engine(PG_DSN)

Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


class SwapiPeople(Base):
    __tablename__ = 'swapi_people'
    id = Column(Integer, primary_key=True)
    json = Column(JSON)


async def download_links(links, client_session):
    coros = [client_session.get(link) for link in links]
    http_responses = await asyncio.gather(*coros)
    json_coros = [http_response.json() for http_response in http_responses]
    return await asyncio.gather(*json_coros)


async def get_people(people_id, client_session):
    if people_id == 17 or people_id > 83:
        return
    async with client_session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        data = {}
        json_data = await response.json()
        films_link = json_data.get('films', [])
        species_link = json_data.get('species', [])
        vehicles_link = json_data.get('vehicles', [])
        starships_link = json_data.get('starships', [])
        films_coro = download_links(films_link, client_session)
        species_coro = download_links(species_link, client_session)
        vehicles_coro = download_links(vehicles_link, client_session)
        starships_coro = download_links(starships_link, client_session)
        fields = await asyncio.gather(films_coro, species_coro, vehicles_coro, starships_coro)
        films, species, vehicles, starships = fields
        data['id'] = people_id
        data['name'] = json_data.get('name', '')
        data['birth_year'] = json_data.get('birth_year', '')
        data['eye_color'] = json_data.get('eye_color', '')
        data['films'] = ','.join([film.get('title', '') for film in films])
        data['gender'] = json_data.get('gender', '')
        data['hair_color'] = json_data.get('hair_color', '')
        data['height'] = json_data.get('height', '')
        data['homeworld'] = json_data.get('homeworld', '')
        data['mass'] = json_data.get('mass', '')
        data['skin_color'] = json_data.get('skin_color', '')
        data['starships'] = ','.join([starship.get('name', '') for starship in starships])
        data['species'] = ','.join([sp.get('name', '') for sp in species])
        data['vehicles'] = ','.join([vehicle.get('name', '') for vehicle in vehicles])
        return data


async def write_to_database(results):
    async with Session() as session:
        orm_objects = [SwapiPeople(json=result) for result in results]
        session.add_all(orm_objects)
        await session.commit()


async def request_block(i):
    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(_id, client_session) for _id in range(i, i + 5)]
        results = await asyncio.gather(*coros)
    asyncio.create_task(write_to_database(results=results))


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)
    coros = [request_block(i) for i in range(1, 84, 5)]
    await asyncio.gather(*coros)


if __name__ == '__main__':
    print('==== S T A R T I N G =====')
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
    print('========= E N D ==========')
