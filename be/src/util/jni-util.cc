// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/jni-util.h"

#include <hdfs.h>
#include <sstream>

#include "common/status.h"
#include "rpc/jni-thrift-util.h"
#include "util/test-info.h"

#include "common/names.h"

namespace impala {

Status JniUtfCharGuard::create(JNIEnv* env, jstring jstr, JniUtfCharGuard* out) {
  DCHECK(jstr != nullptr);
  DCHECK(!env->ExceptionCheck());
  jboolean is_copy;
  const char* utf_chars = env->GetStringUTFChars(jstr, &is_copy);
  bool exception_check = static_cast<bool>(env->ExceptionCheck());
  if (utf_chars == nullptr || exception_check) {
    if (exception_check) env->ExceptionClear();
    if (utf_chars != nullptr) env->ReleaseStringUTFChars(jstr, utf_chars);
    auto fail_message = "GetStringUTFChars failed. Probable OOM on JVM side";
    LOG(ERROR) << fail_message;
    return Status(fail_message);
  }
  out->env = env;
  out->jstr = jstr;
  out->utf_chars = utf_chars;
  return Status::OK();
}

bool JniScopedArrayCritical::Create(JNIEnv* env, jbyteArray jarr,
    JniScopedArrayCritical* out) {
  DCHECK(env != nullptr);
  DCHECK(out != nullptr);
  DCHECK(!env->ExceptionCheck());
  int size = env->GetArrayLength(jarr);
  void* pac = env->GetPrimitiveArrayCritical(jarr, nullptr);
  if (pac == nullptr) {
    LOG(ERROR) << "GetPrimitiveArrayCritical() failed. Probable OOM on JVM side";
    return false;
  }
  out->env_ = env;
  out->jarr_ = jarr;
  out->arr_ = static_cast<uint8_t*>(pac);
  out->size_ = size;
  return true;
}

bool JniUtil::jvm_inited_ = false;
jclass JniUtil::jni_util_cl_ = NULL;
jclass JniUtil::internal_exc_cl_ = NULL;
jmethodID JniUtil::get_jvm_metrics_id_ = NULL;
jmethodID JniUtil::get_jvm_threads_id_ = NULL;
jmethodID JniUtil::get_jmx_json_ = NULL;
jmethodID JniUtil::throwable_to_string_id_ = NULL;
jmethodID JniUtil::throwable_to_stack_trace_id_ = NULL;

Status JniLocalFrame::push(JNIEnv* env, int max_local_ref) {
  DCHECK(env_ == NULL);
  DCHECK_GT(max_local_ref, 0);
  if (env->PushLocalFrame(max_local_ref) < 0) {
    env->ExceptionClear();
    return Status("failed to push frame");
  }
  env_ = env;
  return Status::OK();
}

bool JniUtil::ClassExists(JNIEnv* env, const char* class_str) {
  jclass local_cl = env->FindClass(class_str);
  jthrowable exc = env->ExceptionOccurred();
  if (exc != NULL) {
    env->ExceptionClear();
    return false;
  }
  env->DeleteLocalRef(local_cl);
  return true;
}

bool JniUtil::MethodExists(JNIEnv* env, jclass class_ref, const char* method_str,
    const char* method_signature) {
  env->GetMethodID(class_ref, method_str, method_signature);
  jthrowable exc = env->ExceptionOccurred();
  if (exc != nullptr) {
    env->ExceptionClear();
    return false;
  }
  return true;
}

Status JniUtil::GetGlobalClassRef(JNIEnv* env, const char* class_str, jclass* class_ref) {
  *class_ref = NULL;
  jclass local_cl = env->FindClass(class_str);
  RETURN_ERROR_IF_EXC(env);
  RETURN_IF_ERROR(LocalToGlobalRef(env, local_cl, class_ref));
  env->DeleteLocalRef(local_cl);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status JniUtil::LocalToGlobalRef(JNIEnv* env, jobject local_ref, jobject* global_ref) {
  *global_ref = env->NewGlobalRef(local_ref);
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status JniUtil::CheckAndSetMaxPermSize() {
  // Frontend tests have it set via maven configuration.
  if (TestInfo::is_fe_test()) return Status::OK();
  // Validate the Java version in use based on the supported JNI version. This is not
  // totally accurate since Java 6 and Java 7 use the same JNI spec. However
  // we just need to know whether we are on Java 8 or later so that we can skip
  // setting -XX::MaxPermSize.
  JavaVMInitArgs vm_args;
  vm_args.version = JNI_VERSION_1_8;
  // JNI_GetDefaultJavaVMInitArgs() checks if the linked JVM supports a given
  // JNI version. We noticed that it does not update the input vm_args on return
  // as advertised in the documentation.
  if (JNI_GetDefaultJavaVMInitArgs(&vm_args) == JNI_OK) {
    // Java 8 does not use perm-gen space. Nothing to do.
    return Status::OK();
  }

  // Make sure that the JVM has not already been spawned.
  const jsize vm_buf_len = 1;
  JavaVM* vm_buf[vm_buf_len];
  jint num_vms = 0;
  if (JNI_GetCreatedJavaVMs(&vm_buf[0], vm_buf_len, &num_vms) != JNI_OK) {
    return Status("JNI_GetCreatedJavaVMs() failed.");
  }
  if (num_vms) {
    return Status("JVM has already been spawned. Aborting CheckAndSetMaxPermSize().");
  }
  // We only check for JAVA_TOOL_OPTIONS that the end users are expected to
  // override. Ideally one can also add the VM options to LIBHDFS_OPTS that
  // the libhdfs wrapper accepts but that is only used by the test infrastructure.
  const char* env_var = "JAVA_TOOL_OPTIONS";
  string opts_to_set;
  const char* get_env_result;
  if ((get_env_result = getenv(env_var)) != nullptr) {
    opts_to_set = get_env_result;
  }
  if (opts_to_set.find("-XX:MaxPermSize") == string::npos) {
    // Not set, append to the VM options.
    opts_to_set += " -XX:MaxPermSize=128m";
  } else {
    // Already set, return.
    return Status::OK();
  }
  if (setenv(env_var, opts_to_set.c_str(), 1) < 0) {
    return Status("Error setting -XX:MaxPermSize in JAVA_TOOL_OPTS");
  }
  VLOG(1) << "Using JAVA_TOOL_OPTIONS: " << opts_to_set;
  return Status::OK();
}

Status JniUtil::Init() {
  RETURN_IF_ERROR(CheckAndSetMaxPermSize());
  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = getJNIEnv();
  if (env == NULL) return Status("Failed to get/create JVM");
  // Find JniUtil class and create a global ref.
  jclass local_jni_util_cl = env->FindClass("org/apache/impala/common/JniUtil");
  if (local_jni_util_cl == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil class.");
  }
  jni_util_cl_ = reinterpret_cast<jclass>(env->NewGlobalRef(local_jni_util_cl));
  if (jni_util_cl_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to create global reference to JniUtil class.");
  }
  env->DeleteLocalRef(local_jni_util_cl);
  if (env->ExceptionOccurred()) {
    return Status("Failed to delete local reference to JniUtil class.");
  }

  // Find InternalException class and create a global ref.
  jclass local_internal_exc_cl =
      env->FindClass("org/apache/impala/common/InternalException");
  if (local_internal_exc_cl == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil class.");
  }
  internal_exc_cl_ = reinterpret_cast<jclass>(env->NewGlobalRef(local_internal_exc_cl));
  if (internal_exc_cl_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to create global reference to JniUtil class.");
  }
  env->DeleteLocalRef(local_internal_exc_cl);
  if (env->ExceptionOccurred()) {
    return Status("Failed to delete local reference to JniUtil class.");
  }

  // Throwable toString()
  throwable_to_string_id_ =
      env->GetStaticMethodID(jni_util_cl_, "throwableToString",
          "(Ljava/lang/Throwable;)Ljava/lang/String;");
  if (throwable_to_string_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.throwableToString method.");
  }

  // throwableToStackTrace()
  throwable_to_stack_trace_id_ =
      env->GetStaticMethodID(jni_util_cl_, "throwableToStackTrace",
          "(Ljava/lang/Throwable;)Ljava/lang/String;");
  if (throwable_to_stack_trace_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.throwableToFullStackTrace method.");
  }

  get_jvm_metrics_id_ =
      env->GetStaticMethodID(jni_util_cl_, "getJvmMemoryMetrics", "([B)[B");
  if (get_jvm_metrics_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.getJvmMemoryMetrics method.");
  }

  get_jvm_threads_id_ =
      env->GetStaticMethodID(jni_util_cl_, "getJvmThreadsInfo", "([B)[B");
  if (get_jvm_threads_id_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.getJvmThreadsInfo method.");
  }

  get_jmx_json_ =
      env->GetStaticMethodID(jni_util_cl_, "getJMXJson", "()[B");
  if (get_jmx_json_ == NULL) {
    if (env->ExceptionOccurred()) env->ExceptionDescribe();
    return Status("Failed to find JniUtil.getJMXJson method.");
  }
  jvm_inited_ = true;
  return Status::OK();
}

void JniUtil::InitLibhdfs() {
  // make random libhdfs calls to make sure that the context class loader isn't
  // null; see xxx for an explanation
  hdfsFS fs = hdfsConnect("default", 0);
  hdfsDisconnect(fs);
}

Status JniUtil::InitJvmPauseMonitor() {
  JNIEnv* env = getJNIEnv();
  if (!env) return Status("Failed to get/create JVM.");
  if (!jni_util_cl_) return Status("JniUtil::Init() not called.");
  jmethodID init_jvm_pm_method;
  JniMethodDescriptor init_jvm_pm_desc = {"initPauseMonitor", "()V", &init_jvm_pm_method};
  RETURN_IF_ERROR(JniUtil::LoadStaticJniMethod(env, jni_util_cl_, &init_jvm_pm_desc));
  RETURN_IF_ERROR(JniUtil::CallJniMethod(jni_util_cl_, init_jvm_pm_method));
  return Status::OK();
}

Status JniUtil::GetJniExceptionMsg(JNIEnv* env, bool log_stack, const string& prefix) {
  jthrowable exc = env->ExceptionOccurred();
  if (exc == nullptr) return Status::OK();
  env->ExceptionClear();
  DCHECK(throwable_to_string_id() != nullptr);
  const char* oom_msg_template = "$0 threw an unchecked exception. The JVM is likely out "
      "of memory (OOM).";
  jstring msg = static_cast<jstring>(env->CallStaticObjectMethod(jni_util_class(),
      throwable_to_string_id(), exc));
  if (env->ExceptionOccurred()) {
    env->ExceptionClear();
    string oom_msg = Substitute(oom_msg_template, "throwableToString");
    LOG(ERROR) << oom_msg;
    return Status(oom_msg);
  }
  JniUtfCharGuard msg_str_guard;
  RETURN_IF_ERROR(JniUtfCharGuard::create(env, msg, &msg_str_guard));
  if (log_stack) {
    jstring stack = static_cast<jstring>(env->CallStaticObjectMethod(jni_util_class(),
        throwable_to_stack_trace_id(), exc));
    if (env->ExceptionOccurred()) {
      env->ExceptionClear();
      string oom_msg = Substitute(oom_msg_template, "throwableToStackTrace");
      LOG(ERROR) << oom_msg;
      return Status(oom_msg);
    }
    JniUtfCharGuard c_stack_guard;
    RETURN_IF_ERROR(JniUtfCharGuard::create(env, stack, &c_stack_guard));
    VLOG(1) << c_stack_guard.get();
  }

  env->DeleteLocalRef(exc);
  return Status(Substitute("$0$1", prefix, msg_str_guard.get()));
}

Status JniUtil::GetJvmMemoryMetrics(const TGetJvmMemoryMetricsRequest& request,
    TGetJvmMemoryMetricsResponse* result) {
  return JniUtil::CallJniMethod(jni_util_class(), get_jvm_metrics_id_, request, result);
}

Status JniUtil::GetJvmThreadsInfo(const TGetJvmThreadsInfoRequest& request,
    TGetJvmThreadsInfoResponse* result) {
  return JniUtil::CallJniMethod(jni_util_class(), get_jvm_threads_id_, request, result);
}

Status JniUtil::GetJMXJson(TGetJMXJsonResponse* result) {
  return JniUtil::CallJniMethod(jni_util_class(), get_jmx_json_, result);
}

Status JniUtil::LoadJniMethod(JNIEnv* env, const jclass& jni_class,
    JniMethodDescriptor* descriptor) {
  (*descriptor->method_id) = env->GetMethodID(jni_class,
      descriptor->name.c_str(), descriptor->signature.c_str());
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

Status JniUtil::LoadStaticJniMethod(JNIEnv* env, const jclass& jni_class,
    JniMethodDescriptor* descriptor) {
  (*descriptor->method_id) = env->GetStaticMethodID(jni_class,
      descriptor->name.c_str(), descriptor->signature.c_str());
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}
}
