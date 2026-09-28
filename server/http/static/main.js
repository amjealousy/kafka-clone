import {renderControlPlaneView} from "/static/render.js";

async function requestControlPlane(path) {
    const response = await fetch(`/cluster/v1/${path}`);
    if (!response.ok) throw new Error(`Сервер вернул HTTP ${response.status}`);
    return response.json();
}
const root = document.querySelector("#app");

async function refreshOverview() {
    const overview = await requestControlPlane("overview");
    renderControlPlaneView(overview, root, refreshOverview);
}



async function main() {
    try {
        await refreshOverview();
    } catch (error) {
        console.error(error);
        root.textContent = `Ошибка: ${error.message}`;
    }
}

main();
