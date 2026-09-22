import PQueue from '../source/index.js';
import delay from 'delay';

// 取消排队中的任务 / 取消运行中的任务 / 自定义 queueClass 下取消，三种取消各自的读数。
const probe = async (which: string) => {
	const queue = new PQueue({concurrency: 1});
	const errors: string[] = [];
	const completed: unknown[] = [];
	queue.on('error', (error: Error) => errors.push(error.name || String(error)));
	queue.on('completed', (result) => completed.push(result));

	const acA = new AbortController();
	const acB = new AbortController();

	const a = queue.add(async () => delay(100), {id: 'A', signal: acA.signal}).catch((error: Error) => `A:${error.name}`);
	const b = queue.add(async () => delay(10), {id: 'B', signal: acB.signal}).catch((error: Error) => `B:${error.name}`);

	if (which === 'queued') {
		acB.abort();
	} else {
		await delay(20);
		acA.abort();
	}

	const onErrorRace = await Promise.race([
		queue.onError().then(() => 'error', () => 'error'),
		delay(120).then(() => 'timeout'),
	]);
	const results = [await a, await b];
	await queue.onIdle();

	console.log(JSON.stringify({which, errors, completed, onErrorRace, results, endPending: queue.pending}));
};

for (const which of ['queued', 'running']) {
	// eslint-disable-next-line no-await-in-loop
	await probe(which);
}

class QueueClass {
	_queue: Array<() => void> = [];

	enqueue(run: () => void) {
		this._queue.push(run);
	}

	dequeue() {
		return this._queue.shift();
	}

	get size() {
		return this._queue.length;
	}

	filter() {
		return this._queue;
	}
}

for (const label of ['priority-queue(default)', 'custom queueClass']) {
	const queue = new PQueue(label.startsWith('custom') ? {concurrency: 1, queueClass: QueueClass} : {concurrency: 1});
	const errors: string[] = [];
	const ran: string[] = [];
	queue.on('error', (error: Error) => errors.push(error.name || String(error)));

	const ac = new AbortController();
	queue.add(async () => {
		ran.push('A');
		await delay(20);
	});
	const b = queue.add(async () => {
		ran.push('B-cancelled');
		await delay(20);
	}, {id: 'task-b', signal: ac.signal}).catch((error: Error) => error.name);
	queue.add(async () => {
		ran.push('C');
		await delay(20);
	});
	queue.add(async () => {
		ran.push('D');
		await delay(20);
	});
	ac.abort();
	const bRejection = await b;

	const outcome = await Promise.race([
		queue.onIdle().then(() => 'idle'),
		delay(600).then(() => 'STALLED'),
	]);

	console.log(JSON.stringify({label, bRejection, errors, ran, outcome, size: queue.size, pending: queue.pending}));
}
