use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use chronicle_db::tools::arrays::array::FixedArray;

pub(crate) struct ShadowVec<'a, E: Default> {
    pub(crate) unreal_vec: ManuallyDrop<Vec<E>>,
    pub(crate) p_array: &'a mut FixedArray<E>
}

impl<'a, E: Default> ShadowVec<'a, E> {
    pub(crate) fn new(cap: usize, p_array: &'a mut FixedArray<E>) -> Self {
        unsafe {
            ShadowVec {
                unreal_vec: ManuallyDrop::new(Vec::from_raw_parts(
                    p_array.as_mut_ptr(),
                    p_array.len(),
                    cap)),
                p_array
            }
        }
    }
}

impl<'a, E: Default> Drop for ShadowVec<'a, E> {
    fn drop(&mut self) {
        unsafe {
            self.p_array.set_len(self.unreal_vec.len())
        }
    }
}

impl<'a, E: Default> Deref for ShadowVec<'a, E> {
    type Target = Vec<E>;

    fn deref(&self) -> &Self::Target {
        self.unreal_vec.as_ref()
    }
}

impl<'a, E: Default> DerefMut for ShadowVec<'a, E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.unreal_vec.as_mut()
    }
}

impl<'a, E: Default> Into<ShadowVec<'a, E>> for (usize, &'a mut FixedArray<E>) {
    fn into(self) -> ShadowVec<'a, E> {
        ShadowVec::new(self.0, self.1)
    }
}