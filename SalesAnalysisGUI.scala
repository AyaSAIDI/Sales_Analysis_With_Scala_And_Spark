package SalesAnalysis

import javafx.application.Application
import javafx.scene.Scene
import javafx.scene.control._
import javafx.scene.layout._
import javafx.stage.{FileChooser, Stage}

object SalesAnalysisApp extends App {
  Application.launch(classOf[SalesAnalysisGUI])
}

class SalesAnalysisGUI extends Application {
  override def start(primaryStage: Stage): Unit = {
    // Root layout
    val root = new VBox(10)
    root.setStyle("-fx-padding: 10; -fx-spacing: 10;")

    // File selection buttons
    val customerFileBtn = new Button("Charger Customers")
    val itemFileBtn = new Button("Charger Items")
    val orderFileBtn = new Button("Charger Orders")
    val productFileBtn = new Button("Charger Products")
    val analyzeBtn = new Button("Analyser")
    analyzeBtn.setDisable(true)

    // Labels to show selected files
    val customerFileLabel = new Label("Fichier non sélectionné")
    val itemFileLabel = new Label("Fichier non sélectionné")
    val orderFileLabel = new Label("Fichier non sélectionné")
    val productFileLabel = new Label("Fichier non sélectionné")

    // Text area for results
    val resultArea = new TextArea()
    resultArea.setEditable(false)
    resultArea.setPrefHeight(300)

    // File choosers
    val fileChooser = new FileChooser()
    def selectFile(label: Label): Unit = {
      val selectedFile = fileChooser.showOpenDialog(primaryStage)
      if (selectedFile != null) {
        label.setText(selectedFile.getAbsolutePath)
        if (
          customerFileLabel.getText != "Fichier non sélectionné" &&
            itemFileLabel.getText != "Fichier non sélectionné" &&
            orderFileLabel.getText != "Fichier non sélectionné" &&
            productFileLabel.getText != "Fichier non sélectionné"
        ) {
          analyzeBtn.setDisable(false)
        }
      }
    }

    // Event handlers for file selection
    customerFileBtn.setOnAction(_ => selectFile(customerFileLabel))
    itemFileBtn.setOnAction(_ => selectFile(itemFileLabel))
    orderFileBtn.setOnAction(_ => selectFile(orderFileLabel))
    productFileBtn.setOnAction(_ => selectFile(productFileLabel))

    // Event handler for analysis
    analyzeBtn.setOnAction(_ => {
      resultArea.setText("Analyse en cours...\n")

      // Call the analyzeData function
      val analysisResult = DataAnalyzer.analyzeData(
        customerFileLabel.getText,
        itemFileLabel.getText,
        orderFileLabel.getText,
        productFileLabel.getText
      )

      resultArea.setText(analysisResult)
    })

    // Layout configuration
    val fileSelectionLayout = new VBox(10,
      new HBox(10, customerFileBtn, customerFileLabel),
      new HBox(10, itemFileBtn, itemFileLabel),
      new HBox(10, orderFileBtn, orderFileLabel),
      new HBox(10, productFileBtn, productFileLabel),
      analyzeBtn
    )
    root.getChildren.addAll(fileSelectionLayout, new Label("Résultats :"), resultArea)

    // Scene configuration
    val scene = new Scene(root, 600, 500)
    primaryStage.setTitle("Sales Analysis")
    primaryStage.setScene(scene)
    primaryStage.show()
  }
}
