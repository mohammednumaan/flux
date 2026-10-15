import http from 'k6/http';
import { check } from 'k6';
import { Rate, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://10.104.169.72:8090';
const PATH = '/api';

const successful = new Rate('flux_requests_successful');
const failed = new Counter('flux_requests_failed');

export const options = {
    discardResponseBodies: true,
    scenarios: {
        load_ramp: {
            executor: 'ramping-arrival-rate',
            startRate: 100,
            timeUnit: '1s',
            stages: [
                { target: 250, duration: '1m' },
                { target: 500, duration: '1m' },
                { target: 750, duration: '1m' },
                { target: 1000, duration: '1m' },
                { target: 1250, duration: '1m' },
                { target: 1500, duration: '1m' },
                { target: 1500, duration: '4m' },
                { target: 0, duration: '5m' },
            ],
            preAllocatedVUs: 800,
            maxVUs: 4000,
            gracefulStop: '30s',
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<1500', 'p(99)<3000'],
        dropped_iterations: ['count==0'],
    },
};

export default function() {
    const res = http.get(`${BASE_URL}${PATH}`, { timeout: '10s' });
    const ok = check(res, { 'status is 200': (r) => r.status === 200 });
    successful.add(ok);
    if (!ok) {
        failed.add(1);
    }
}
