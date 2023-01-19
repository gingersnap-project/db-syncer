package io.gingersnapproject.k8s.informer;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;

public interface KubernetesInformer<T> extends ResourceEventHandler<T> {

   SharedIndexInformer<T> register(KubernetesClient kc);
}
