package io.gingersnapproject.util;

public class ByRef<T> {

   private volatile T ref;

   public ByRef(T value) {
      this.ref = value;
   }

   public ByRef() {
      this(null);
   }

   public T ref() {
      return ref;
   }

   public void setRef(T ref) {
      this.ref = ref;
   }
}
