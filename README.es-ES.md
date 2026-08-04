# Flink-Alt

## Resumen

*Flink-Alt* es una API alternativa minimalista basada en typeclasses para Flink, que abstrae sobre el tipo de flujo de datos, permitiendo implementaciones alternativas. Además de la implementación para Flink `DataStream`, también proporcionamos una implementación adicional en memoria, útil para propósitos de prueba donde no desea desperdiciar segundos preciosos para que Flink inicie y detenga un clúster en memoria.

*Flink-Alt* actualmente se enfoca solo en trabajos de streaming (la API `DataStream`) usando *event time*. Las operaciones soportadas incluyen operaciones ordinarias como `map`, `flatMap` y `filter`, y también incluye operaciones con estado como `mapWithState` y operaciones como `reduce`.

## Introducción

La biblioteca utiliza el enfoque de diseño *tagless final* (consulte [Exploring Tagless Final](https://blog.scalac.io/exploring-tagless-final.html) [Alternatives to GADTs in Scala](https://pchiusano.github.io/2014-05-20/scala-gadts.html) o [Optimizing Tagless Final](https://typelevel.org/blog/2017/12/27/optimizing-final-tagless.html), o una de las muchas excelentes charlas en YouTube).

Una aplicación de ejemplo se ve así:
```scala
import cats.data.State
import com.github.flinkalt.{DStream, Stateful, StateTrans}
import DStream.ops._
import Stateful.ops._

case class Count[T](value: T, count: Int)

def totalWordCount[DS[_] : DStream : Stateful](lines: DS[String]): DS[Count[String]] = {
  lines
    .flatMap(splitToWords)
    .mapWithState(zipWithCount)
}

def splitToWords(line: String): Seq[String] = {
  line.toLowerCase().split("\\W+").filter(_.nonEmpty)
}

def zipWithCount[T]: StateTrans[T, Int, T, Count[T]] = {
  StateTrans(
    identity,
    t => State(maybeCount => {
      val count = maybeCount.getOrElse(0) + 1
      (Some(count), Count(t, count))
    }))
}
```
 
## Información de Tipo

*Flink-Alt* también ofrece una alternativa a [la derivación de información de tipo basada en macros de Flink](https://ci.apache.org/projects/flink/flink-docs-release-1.5/dev/types_serialization.html#type-information-in-the-scala-api). La derivación de información de tipo está basada en [shapeless](https://github.com/milessabin/shapeless), y está inspirada en gran medida en [flink-shapeless](https://github.com/joroKr21/flink-shapeless). Sus características principales incluyen:
 - todo se asume inmutable
 - derivación automática de información de tipo:
    - primitivas: `Boolean`, `Byte`, `Short`, `Char`, `Int`, `Long`, `Float`, `Double`, `Unit` 
    - tipos comunes: `String`, `Option`, `Either`, `cats.data.Validated`)
    - arreglos de bytes
    - algunas colecciones: `Seq`, `IndexedSeq`, `List`, `Vector`, `Set`, `Map` y `ListMap` (no para todos `Traversable` porque las instancias de `CanBuild` de scala no son serializables)
    - inyecciones, es decir, representar algunos tipos como otro que tiene `TypeInfo`
    - ADTs, usando *shapeless*, es decir, cualquier case class o trait sellado para el cual *shapeless* puede derivar `Generic`  
 - deduplicación de datos:
    - cuando un gráfico de objetos se serializa con valores iguales en él (como cadenas iguales, colecciones o prácticamente cualquier valor), entonces estos valores se representan una sola vez, ahorrando espacio y tiempo de serialización   
    - cuando los datos se desean, los valores duplicados no se asignan múltiples veces en memoria, reduciendo así los requisitos de memoria
    - la deduplicación se basa en los métodos `equals` y `hashCode` de los objetos, así que debe implementarlos eficientemente       
 

### Ejemplo:

```scala
import org.apache.flink.streaming.api.datastream.DataStream
import com.github.flinkalt.typeinfo.auto._

case class Count[T](value: T, count: Int)

val s1: DataStream[String] = ???
val s2: DataStream[Count[String]] = s1.map(s => Count(s, 1)) // DataStream.map requiere un TypeInformation implícito para la salida, Count[String] en este caso

```
