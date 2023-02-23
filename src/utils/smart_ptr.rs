use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

unsafe impl<E> Send for SmartPtr<E> { }
unsafe impl<E> Sync for SmartPtr<E> { }


struct SmartInner<E> {
    sc: AtomicUsize,
    data: E
}

#[repr(C)]
pub struct SmartPtr<E>(NonNull<SmartInner<E>>);

impl<E> Clone for SmartPtr<E> {
    fn clone(&self) -> Self {
        unsafe {
            self.0.as_ref().sc.fetch_add(1, Relaxed);
        }

        Self(self.0)
    }
}

impl<E> Drop for SmartPtr<E> {
    fn drop(&mut self) {
        unsafe{
            if self.0.as_ref().sc.fetch_sub(1, Release) != 1 {
                return;
            }

            ptr::drop_in_place(&mut self.0.as_mut().data);
        }
    }
}

impl<E> SmartPtr<E> {
    pub fn new(data: E) -> Self {
        unsafe {
            Self(NonNull::new_unchecked(Box::leak(Box::new(SmartInner {
                    sc: AtomicUsize::new(1),
                    data,
                }))))
        }
    }

    pub fn clone_result(&self) -> Option<Self> {
        unsafe {
            let smart = self.0.as_ref();
            let sc =  smart.sc.load(Relaxed);
            if sc == 0 || sc > isize::MAX as _ {
                None
            }
            else {
                smart.sc
                    .compare_exchange(sc, sc + 1, Relaxed, Relaxed)
                    .ok()
                    .and(Some(Self(self.0)))
            }
        }
    }
}
