package com.lj.test;

public class MyThread extends Thread {
	@Override
	public void run() {
		System.out.println(Thread.currentThread().getName() + "正在执行。。。"+Thread.currentThread().getThreadGroup().getName());
	}
}
