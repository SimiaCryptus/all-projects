## Java

* DropoutNoiseLayerTest$Basic
    ```
    java.lang.AssertionError: ToleranceStatistics
    ```
* ImgTileSubnetLayerTest$Overlapping
    ```
	java.lang.AssertionError: [6, 6, 1] != [8, 8, 1]
    ```
* NthPowerActivationLayerTest$ZeroPowerTest
    ```
	java.lang.RuntimeException: Frozen component did not pass input backwards
    ```
* ScaleMetaLayerTest$Basic
    ```
	java.lang.AssertionError: ToleranceStatistics
    ```
* SumInputsLayerTest$OnePlusOne
    ```
	java.lang.AssertionError: ToleranceStatistics
    ```

	

## CuDNN

* BinarySumLayerTest$OnePlusOne
    ```
	RefLeak logged 2318 bytes
    ```
* ConvolutionLayerTest$BandExpand
    ```
	java.lang.IllegalStateException: Object com.simiacryptus.mindseye.test.unit.StandardLayerTests$Invocation
    ```
* ConvolutionLayerTest$Big1
    ```
	RefLeak logged 2776 bytes
    ```
* ImgModulusPaddingLayerTest$Basic
    ```
	java.lang.AssertionError: ToleranceStatistics
    ```
* ImgModulusPaddingSubnetLayerTest$Basic
    ```
	java.lang.AssertionError: ToleranceStatistics
    ```
* ImgTileSubnetLayerTest$Basic
    ```
	java.lang.AssertionError: ToleranceStatistics
    ```
* SumInputsLayerTest$OnePlusOne
    ```
	java.lang.IllegalStateException at com.simiacryptus.mindseye.test.SimpleEval.checkedFeedback(SimpleEval.java:101)
    ```

## Aparapi

* com.simiacryptus.mindseye.layers.aparapi.ConvolutionLayerTest.Basic
    ```
	java.lang.AssertionError: user supplied Device incompatible with current EXECUTION_MODE or getTargetDevice();
    ```

## TensorFlow

* BiasLayerTest
    ```
	java.lang.IllegalArgumentException: Add
    ```
* ConvTFMnist$LayerTest
    ```
	java.lang.IllegalArgumentException: transpose expects a vector of size 3. But input(1) is a vector of size 4
    ```
* CudnnJavaMnist$LayerTest
    ```
	java.lang.ArrayIndexOutOfBoundsException: 2
    ```
* FloatTFMnist$LayerTest
    ```
	java.lang.UnsupportedOperationException: JsonNull
    ```
* InceptionPipelineTest$Layer2
    ```
	org.tensorflow.TensorFlowException: No gradient defined for op: Concat
    ```
* InceptionPipelineTest$Layer3
    ```
	org.tensorflow.TensorFlowException: No gradient defined for op: Concat
    ```
* InceptionPipelineTest$Layer4
    ```
	org.tensorflow.TensorFlowException: No gradient defined for op: Concat
    ```
* InceptionPipelineTest$Layer5
    ```
	org.tensorflow.TensorFlowException: No gradient defined for op: Concat
    ```
* InceptionPipelineTest$Layer6
    ```
	org.tensorflow.TensorFlowException: No gradient defined for op: Concat
    ```
* InceptionPipelineTest$Layer7
    ```
	org.tensorflow.TensorFlowException: No gradient defined for op: Concat
    ```
* InceptionPipelineTest$Layer8
    ```
	org.tensorflow.TensorFlowException: No gradient defined for op: Concat
    ```
* InceptionPipelineTest$Layer9
    ```
	org.tensorflow.TensorFlowException: No gradient defined for op: Concat
    ```
* InceptionPipelineTest$Layer10
    ```
	org.tensorflow.TensorFlowException: No gradient defined for op: Concat
    ```
* MatMulLayerTest
    ```
	java.lang.IllegalArgumentException: Reshape
    ```
* SimpleConvTFMnist$LayerTest
    ```
	java.lang.UnsupportedOperationException: JsonNull
    ```
* SimpleCudnnMnist$LayerTest
    ```
	java.lang.IllegalArgumentException at com.simiacryptus.mindseye.layers.cudnn.conv.SimpleConvolutionLayer.getOutputSize(SimpleConvolutionLayer.java:475)
    ```
* SimpleTFMnist$LayerTest
    ```
	java.lang.UnsupportedOperationException: JsonNull
    ```
* SoftmaxLayerTest
    ```
	java.lang.IllegalArgumentException: Softmax
    ```

## Research

## Art