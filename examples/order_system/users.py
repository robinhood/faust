# site/users.py
import faust
from .app import app


class User(faust.Record):
    id: int
    email: str
    first_name: str
    last_name: str

    objects = app.Table()


class UserAndUrl(faust.Record):
    user_id: id
    url: str


@app.agent(value_type=User)
async def register(users):
    async for user in users.group_by(User.id):
        print(f'Register user: {user}')
        User.objects.setdefault(user.id, user)


@app.command()
async def add_example_users():
    """Add example users."""
    await register.send(value=User(
        1, 'george@vandelay.com', 'George', 'Costanza',
    ))
    await register.send(value=User(
        2, 'elaine@benes.com', 'Elaine Marie', 'Benes',
    ))
    await register.send(value=User(
        3, 'jerry@seinfeld.com', 'Jerry', 'Seinfeld',
    ))
