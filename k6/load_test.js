import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8090';

export const options = {
    scenarios: {
        ramp: {
            executor: 'ramping-arrival-rate',
            startRate: 100,
            timeUnit: '1s',
            stages: [
                { target: 200, duration: '2m' },
                { target: 500, duration: '2m' },
                { target: 800, duration: '2m' },
                { target: 800, duration: '5m' },
                { target: 400, duration: '2m' },
                { target: 0, duration: '2m' },
            ],
            preAllocatedVUs: 500,
            maxVUs: 3000,
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<1500', 'p(99)<3000'],
        dropped_iterations: ['count==0'],
    },
};

export default function() {
    const res = http.get(`${BASE_URL}/api`, { timeout: '10s' });
    check(res, { 'status is 200': (r) => r.status === 200 });
}
