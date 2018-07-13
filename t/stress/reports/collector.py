from collections import Counter, defaultdict
from . import states
from .app import get_reporting_app, get_reporting_topic
from .web import env

app = get_reporting_app()

reporting_topic = get_reporting_topic(app)

report_index = {}
report_by_category = defaultdict(lambda: defaultdict(dict))
unique_servers = set()
state_counts = Counter()

color_for_state = {
    states.OK: '#007BFF',
    states.FAIL: '#19A9D5',
    states.SLOW: '#8fc9fb',
    states.STALL: '#1909E5',
    states.UNASSIGNED: '#003f3c',
    states.REBALANCING: '#005f3c',
    states.PAUSED: '#003f3c',
}


@app.agent(reporting_topic)
async def process_report(reports):
    async for r in reports:
        report_by_category[r.app_id][r.category][r.hostname] = r.details
        previous = report_index.get(r.key)
        report_index[r.key] = r
        if previous is not None and previous.state != r.state:
            print(f'{r.key} changed {previous.state} -> {r.state}')
        unique_servers.add((r.app_id, r.hostname))
        state_counts[r.state] += 1


@app.page('/dashboard/')
async def dashboard(web, request, template_name='dashboard.html'):
    template = env.get_template(template_name)
    return web.html(template.render({
        'total_workers': len(unique_servers),
        'report': report_by_category,
        'state_counts': [
            {
                'name': state,
                'count': count,
                'color': color_for_state[state],
            }
            for state, count in state_counts.items()
        ],
    }))


@app.page('/dashboard/api/')
async def dashboard_json(web, request):
    return web.json({'status': report_by_category})


if __name__ == '__main__':
    app.main()
