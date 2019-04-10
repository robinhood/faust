from faust import cli
from ..app import app


@app.command(
    cli.option('--side', default='sell', help='Order side: buy, sell'),
    cli.option('--base-url', default='http://localhost:6066'),
)
async def post_order(self: cli.AppCommand, side: str, base_url: str) -> None:
    path = self.app.web.url_for('orders:init', side=side)
    url = ''.join([base_url.rstrip('/'), path])
    async with self.app.http_client.get(url) as response:
        response.raise_for_status()
        self.say(await response.read())
