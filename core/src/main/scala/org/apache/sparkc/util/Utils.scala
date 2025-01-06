package org.apache.sparkc.util

import java.util.Properties

private[spark] object Utils {
  def cloneProperties(props: Properties): Properties = {
    if (props == null) {
      return props
    }
    val resultProps = new Properties()
    props.forEach((k, v) => resultProps.put(k, v))
    resultProps
  }
}
