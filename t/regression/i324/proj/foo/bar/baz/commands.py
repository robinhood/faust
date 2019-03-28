from proj import app


@app.command()
async def foo():
    print('HELLO WORLD')
