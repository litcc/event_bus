use futures::future::BoxFuture;
use futures::task::AtomicWaker;
use log::trace;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::task::{Context, Poll};

pub trait AsyncFn<'a, IT, OT> {
    fn async_call(&self, args: IT) -> BoxFuture<'a, OT>
    where
        Self: 'a;
}

impl<'a, T, F, IT, OT> AsyncFn<'a, IT, OT> for T
where
    F: Future<Output = OT> + 'a + Send + Sync,
    T: Fn(IT) -> F + 'a,
{
    #[inline]
    fn async_call(&self, args: IT) -> BoxFuture<'a, OT>
    where
        Self: 'a,
    {
        Box::pin(self(args))
    }
}

pub trait AsyncFnOnce<'a, IT, OT> {
    fn async_call_once(self: Box<Self>, args: IT) -> BoxFuture<'a, OT>
    where
        Self: 'a;
}

impl<'a, T, F, IT, OT> AsyncFnOnce<'a, IT, OT> for T
where
    F: Future<Output = OT> + 'a + Send + Sync,
    T: FnOnce(IT) -> F + 'a + ?Sized,
{
    #[inline]
    fn async_call_once(self: Box<Self>, args: IT) -> BoxFuture<'a, OT>
    where
        Self: 'a,
    {
        Box::pin(self(args))
    }
}

pub trait AsyncFnMut<'a, IT, OT> {
    fn async_call_mut(&mut self, args: IT) -> BoxFuture<'a, OT>
    where
        Self: 'a;
}

impl<'a, T, F, IT, OT> AsyncFnMut<'a, IT, OT> for T
where
    F: Future<Output = OT> + 'a + Send + Sync,
    T: FnMut(IT) -> F + 'a,
{
    #[inline]
    fn async_call_mut(&mut self, args: IT) -> BoxFuture<'a, OT>
    where
        Self: 'a,
    {
        // Box::pin(self.call_once((args, )))
        Box::pin(self(args))
    }
}

pub struct SuspendCoroutineCall<T>
where
    T: Clone + Send,
{
    return_data: Arc<futures::lock::Mutex<Option<T>>>,
    has_reply: Arc<AtomicBool>,
    waker: Arc<AtomicWaker>,
}

impl<T> Future for SuspendCoroutineCall<T>
where
    T: Clone + Send,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        trace!("SuspendCoroutineCall poll");
        if self.has_reply.load(Relaxed) {
            return Poll::Ready(());
        }

        self.waker.register(cx.waker());

        // Need to check condition **after** `register` to avoid a race
        // condition that would result in lost notifications.
        if self.has_reply.load(Relaxed) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<T> Default for SuspendCoroutineCall<T>
where
    T: Clone + Send,
{
    fn default() -> Self {
        SuspendCoroutineCall {
            return_data: Arc::new(futures::lock::Mutex::new(None)),
            has_reply: Arc::new(AtomicBool::new(false)),
            waker: Arc::new(AtomicWaker::new()),
        }
    }
}

impl<T> Clone for SuspendCoroutineCall<T>
where
    T: Clone + Send,
{
    fn clone(&self) -> Self {
        SuspendCoroutineCall {
            return_data: Arc::clone(&self.return_data),
            has_reply: Arc::clone(&self.has_reply),
            waker: Arc::clone(&self.waker),
        }
    }
}

impl<T> SuspendCoroutineCall<T>
where
    T: Clone + Send,
{
    pub fn resume(&self, res: Option<T>) {
        if !self.has_reply.load(Relaxed) {
            if let Some(mut d) = self.return_data.try_lock() {
                *d = res;
                self.has_reply.store(true, Relaxed);
                self.waker.wake();
            } else {
                self.resume(res);
            }
        }
    }
}

pub async fn suspend_coroutine<T, OP, OT>(fnc: OP) -> Option<T>
where
    OT: Future<Output = ()> + 'static + Sync + Send,
    OP: FnOnce(SuspendCoroutineCall<T>) -> OT + 'static + Sync + Send,
    T: Clone + Send,
{
    let call_fn = SuspendCoroutineCall::default();
    let fnc_box = Box::new(fnc);

    let adf2: BoxFuture<()> = fnc_box.async_call_once(call_fn.clone());
    futures::join!(call_fn.clone(), adf2);
    let result = call_fn.return_data.lock().await;
    return result.clone();
}

// // #[test]
// #[tokio::test]
// async fn test_suspend_coroutine() {
//     println!("开始执行耗时任务");
//     let test_1 = suspend_coroutine(move |result| async move {
//         println!("执行耗时任务中");
//         tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
//         println!("执行耗时任务中 - 准备返回结果");
//         result.resume(Some("".to_string()));
//     })
//     .await;
//     println!("执行耗时任务结束: {:?}", test_1);
//
//     tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
//
//     println!("结束");
// }
