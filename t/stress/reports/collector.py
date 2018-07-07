from collections import defaultdict
from .app import get_reporting_app, get_reporting_topic

app = get_reporting_app()

reporting_topic = get_reporting_topic(app)

report_index = {}
report_by_category = defaultdict(lambda: defaultdict(dict))


@app.agent(reporting_topic)
async def process_report(reports):
    async for r in reports:
        report_by_category[r.app_id][r.category][r.hostname] = r.details
        previous = report_index.get(r.key)
        report_index[r.key] = r
        if previous is not None and previous.state != r.state:
            print(f'{r.key} changed {previous.state} -> {r.state}')


@app.page('/test/report/')
async def get_status(web, request):
    return web.json({'status': report_by_category})


if __name__ == '__main__':
    app.main()
