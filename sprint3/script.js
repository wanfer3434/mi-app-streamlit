document.getElementById("prediction-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const formData = new FormData(e.target);
    const jsonData = {};
    formData.forEach((value, key) => {
        jsonData[key] = value;
    });

    console.log("Datos enviados:", jsonData); // Para depuración

    const response = await fetch("/predict/", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(jsonData),
    });

    if (response.ok) {
        const result = await response.json();
        console.log("Predicción recibida:", result); // Para depuración
        alert(`Restaurante sugerido: ${result.name}, Estrellas: ${result.stars_y}`);
    } else {
        console.error("Error en la predicción:", await response.text());
    }
});
