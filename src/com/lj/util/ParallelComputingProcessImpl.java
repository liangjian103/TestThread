package com.lj.util;

import java.util.List;

public class ParallelComputingProcessImpl implements ParallelComputingProcess<String, String> {

	@Override
	public void process(int threadId, List<String> list, String obj) {
		System.out.println(Thread.currentThread().getThreadGroup().getName()+"-"+Thread.currentThread().getName()+"-threadId["+threadId+"],obj>>>"+obj+"----test>>"+list);
	}

}
