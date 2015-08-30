package com.qualia.keystore_graph;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.parboiled.common.Preconditions;

public abstract class PoolWorkerManager<Context extends Closeable, InputT, OutputT> {

	private ExecutorService service;
	private AtomicBoolean done = new AtomicBoolean();
	private BlockingQueue<InputT> inputQueue = new ArrayBlockingQueue<InputT>(1000);
	private BlockingQueue<OutputT> outputQueue = new ArrayBlockingQueue<OutputT>(2000);

	public abstract Context createContext();

	public abstract OutputT transformInThread(Context context, InputT next);

	public void addInput(InputT input) throws InterruptedException {
		Preconditions.checkArgNotNull(input, "input can not be null");
		while (!inputQueue.offer(input)) {
			checkConsume();
		}
	}

	public abstract void consume(OutputT output);

	public void start(int numThread) {
		service = Executors.newFixedThreadPool(numThread);

		// Create a bunch of worker threads
		for (int i = 0; i < numThread; i++) {
			Context context = createContext();
			WorkerThread thread = new WorkerThread(this, context, done, inputQueue, outputQueue);
			service.submit(thread);
		}
	}

	public void stop() throws InterruptedException {
		checkConsume();
		done.set(true);
		service.shutdown();
		service.awaitTermination(100, TimeUnit.DAYS);
		checkConsume();
	}

	private void checkConsume() {
		List<OutputT> drained = new ArrayList<OutputT>();
		outputQueue.drainTo(drained);
		for (OutputT oneOutput : drained) {
			consume(oneOutput);
		}
	}

	private static class WorkerThread<Context extends Closeable, InputT, OutputT> implements Runnable {

		private final PoolWorkerManager<Context, InputT, OutputT> parent;
		private final Context context;
		private final AtomicBoolean done;
		private final BlockingQueue<InputT> inputQueue;
		private final BlockingQueue<OutputT> outputQueue;

		public WorkerThread(PoolWorkerManager<Context, InputT, OutputT> parent, Context context, AtomicBoolean done, BlockingQueue<InputT> inputQueue,
				BlockingQueue<OutputT> outputQueue) {
			this.parent = parent;
			this.context = context;
			this.done = done;
			this.inputQueue = inputQueue;
			this.outputQueue = outputQueue;
		}

		@Override
		public void run() {
			while (true) {
				try {
					InputT next = inputQueue.poll(1, TimeUnit.MILLISECONDS);
					if ((next == null) && (done.get() == true))
						break;
					if (next != null) {
						OutputT output = parent.transformInThread(context, next);
						outputQueue.put(output);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
			try {
				context.close();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}

}
