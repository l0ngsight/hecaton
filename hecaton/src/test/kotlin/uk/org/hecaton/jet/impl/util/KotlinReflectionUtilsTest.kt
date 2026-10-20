package uk.org.hecaton.jet.impl.util

import uk.org.hecaton.test.HecatonParallelClassRunner
import uk.org.hecaton.test.annotation.ParallelJVMTest
import uk.org.hecaton.test.annotation.QuickTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith

@RunWith(HecatonParallelClassRunner::class)
@Category(QuickTest::class, ParallelJVMTest::class)
class KotlinReflectionUtilsTest {

    @Test
    fun shouldAddNestedAndAnonymousClasses() {
        // When
        val classes = ReflectionUtils.nestedClassesOf(OuterClass::class.java)

        // Then
        assertThat(classes).hasSize(4)
        assertThat(classes).containsExactlyInAnyOrder(
                OuterClass::class.java,
                OuterClass.NestedClass::class.java,
                Class.forName("uk.org.hecaton.jet.impl.util.KotlinReflectionUtilsTest\$OuterClass\$method\$1"),
                Class.forName("uk.org.hecaton.jet.impl.util.KotlinReflectionUtilsTest\$OuterClass\$method\$lambda\$1")
        )
    }

    @Suppress("unused", "UNUSED_VARIABLE")
    class OuterClass {
        private fun method() {
            object : Any() {}

            // JvmSerializableLambdaJvmSerializableLambda to force anonymous class creation
            // Ensures Kotlin 2.0 mimics 1.x behaviour
            // https://kotlinlang.org/docs/whatsnew20.html#generation-of-lambda-functions-using-invokedynamic
            val lambda = @JvmSerializableLambda { }
        }

        class NestedClass
    }
}
