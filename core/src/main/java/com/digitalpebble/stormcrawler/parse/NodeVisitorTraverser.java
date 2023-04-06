package com.digitalpebble.stormcrawler.parse;

import org.jetbrains.annotations.Contract;
import org.jsoup.helper.Validate;
import org.jsoup.internal.StringUtil;
import org.jsoup.nodes.CDataNode;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.NodeVisitor;

public class NodeVisitorTraverser {
    /**
     //     * Start a depth-first traverse of the root and all of its descendants.
     //     *
     //     * @param visitor Node visitor.
     //     * @param root the root node point to traverse.
     //     */
    public static void traverse(
            NodeVisitor visitor, Node root, int maxSize, StringBuilder builder) {
        Validate.notNull(visitor, "null visitor in traverse method");
        Validate.notNull(root, "null root node in traverse method");
        Node node = root;
        int depth = 0;

        while (node != null) {
            // interrupts if too much text has already been produced
            if (maxSize > 0 && builder.length() >= maxSize) return;

            Node parent =
                    node.parentNode(); // remember parent to find nodes that get replaced in .head
            int origSize = parent != null ? parent.childNodeSize() : 0;
            Node next = node.nextSibling();

            visitor.head(node, depth); // visit current node
            if (parent != null && !node.hasParent()) { // removed or replaced
                if (origSize == parent.childNodeSize()) { // replaced
                    node =
                            parent.childNode(
                                    node.siblingIndex()); // replace ditches parent but keeps
                    // sibling index
                } else { // removed
                    node = next;
                    if (node == null) { // last one, go up
                        node = parent;
                        depth--;
                    }
                    continue; // don't tail removed
                }
            }

            if (node.childNodeSize() > 0) { // descend
                node = node.childNode(0);
                depth++;
            } else {
                // when no more siblings, ascend
                while (node.nextSibling() == null && depth > 0) {
                    visitor.tail(node, depth);
                    node = node.parentNode();
                    depth--;
                }
                visitor.tail(node, depth);
                if (node == root) break;
                node = node.nextSibling();
            }
        }
    }

    static void appendNormalisedText(final StringBuilder accum, final TextNode textNode) {
        final String text = textNode.getWholeText();
        if (textNode instanceof CDataNode || preserveWhitespace(textNode.parent()))
            accum.append(text);
        else StringUtil.appendNormalisedWhitespace(accum, text, lastCharIsWhitespace(accum));
    }
        @Contract("null -> false")
    static boolean preserveWhitespace(Node node) {
        if (node == null) return false;
        // looks only at this element and five levels up, to prevent recursion &
        // needless stack searches
        if (node instanceof Element) {
            Element el = (Element) node;
            int i = 0;
            do {
                if (el.tag().preserveWhitespace()) return true;
                el = el.parent();
                i++;
            } while (i < 6 && el != null);
        }
        return false;
    }
    static boolean lastCharIsWhitespace(StringBuilder sb) {
        return sb.length() != 0 && sb.charAt(sb.length() - 1) == ' ';
    }
}
