package com.lj.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCachedThreadPool {
	public static void main(String[] args) {
		// 创建一个可重用固定线程数的线程池
		ExecutorService pool = Executors.newCachedThreadPool(new ThreadFactory(){
			AtomicInteger a = new AtomicInteger();//原子记数
			@Override
			public Thread newThread(Runnable r) {
				ThreadGroup g = new ThreadGroup("TT");
				Thread t = new Thread(g,r);
				t.setName("Test--"+a.getAndIncrement());
				return t;
			}
		});
		
		// 创建实现了Runnable接口对象，Thread对象当然也实现了Runnable接口
		Thread t1 = new MyThread();
		Thread t2 = new MyThread();
		Thread t3 = new MyThread();
		Thread t4 = new MyThread();
		Thread t5 = new MyThread();
		// 将线程放入池中进行执行
		pool.execute(t1);
		pool.execute(t2);
		pool.execute(t3);
		pool.execute(t4);
		pool.execute(t5);
		// 关闭线程池
		pool.shutdown();
	}

}
