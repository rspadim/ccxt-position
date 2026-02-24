import React from "react";
import template from "./app-template.html?raw";

export default function App(): React.JSX.Element {
  return <div id="app" dangerouslySetInnerHTML={{ __html: template }} />;
}
